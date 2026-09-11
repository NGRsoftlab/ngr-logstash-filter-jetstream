# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/json"
require "openssl"
require "nats/client"
require "uri"
require "concurrent"

class LogStash::Filters::Jetstream < LogStash::Filters::Base
  config_name "jetstream"

  config :hosts, :validate => :array, :default => ["nats://localhost:4222"]
  config :bucket, :validate => :string, :required => false
  config :get, :validate => :hash, :required => false
  config :set, :validate => :hash, :required => false
  config :requests, :validate => :string, :required => false
  config :tls_certificate, :validate => :path
  config :tls_enabled, :validate => :boolean, :default => false
  config :tls_version, :validate => %w[TLSv1.1 TLSv1.2 TLSv1.3], :default => 'TLSv1.2'
  config :tls_verification_mode, :validate => %w[full none], :default => 'full'
  config :tag_on_failure, :validate => :string, :default => "_jetstream_failure"

  # Список бакетов, которые нужно кэшировать (например alertix_hostname_to_tags и т.д.)
  config :cache_buckets, :validate => :array, :default => []
  # Интервал обновления кэша в секундах
  config :cache_refresh_interval, :validate => :number, :default => 60

   def mask_passwords_in_urls(urls)
    return urls unless urls.is_a?(Array)

    urls.map do |url_string|
      begin
        uri = URI.parse(url_string.to_s)
        if uri.password
          uri.password = '******'
          uri.to_s
        else
          url_string
        end
      rescue
        url_string
      end

  def register
    # bucket обязателен только если используются get/set
    if (@get&.any? || @set&.any?) && (@bucket.nil? || @bucket.empty?)
      raise LogStash::ConfigurationError,
            "'bucket' is required when 'get' or 'set' options are configured"
    end

    @connection_mutex = Mutex.new
    @jetstream_hosts = validate_connection_hosts
    @jetstream_options = validate_connection_options
    @nc = new_connection(@jetstream_hosts, @jetstream_options)
    @connected = Concurrent::AtomicBoolean.new(true)

    # Кэш: { bucket_name => Concurrent::Map { key => parsed_value } }
    @kv_cache = Concurrent::Map.new
    @cache_buckets.each do |b|
      @kv_cache[b] = Concurrent::Map.new
    end

    start_cache_refresher
  rescue => e
    logger.error("Failed to connect to Jetstream",
                 hosts: mask_passwords_in_urls(@jetstream_hosts),
                 options: @jetstream_options,
                 message: e.message)
    @connected = Concurrent::AtomicBoolean.new(false)
  end

  def filter(event)
    unless connection_available?
      event.tag(@tag_on_failure)
      return
    end

    begin
      process_requests(event)

      set_success = do_set(event)
      get_success = do_get(event)
      filter_matched(event) if set_success || get_success
    rescue => e
      handle_unexpected_error(event, e)
    end
  end

  def close
    @cache_refresher&.shutdown
    @connection_mutex.synchronize do
      @connected.make_false
      @nc&.close
    end
  rescue => e
    logger.debug("Error closing Jetstream connection", message: e.message)
  end

  private

  # ---------- Кэш ----------

  def start_cache_refresher
    return if @cache_buckets.empty?

    # Первичная загрузка — синхронно, чтобы события сразу видели данные
    refresh_cache

    @cache_refresher = Concurrent::TimerTask.new(execution_interval: @cache_refresh_interval) do
      refresh_cache
    end
    @cache_refresher.execute
  end

  def refresh_cache
    @cache_buckets.each do |bucket_name|
      begin
        kv = @jetstream.key_value(bucket_name)
      rescue => e
        logger.warn("jetstream: cannot open bucket for cache", bucket: bucket_name, error: e.message)
        next
      end

      new_map = Concurrent::Map.new
      count = 0
      begin
        kv.keys.each do |key|
          entry = kv.get(key)
          next unless entry
          parsed = parse_value(entry[:value])
          new_map[key] = parsed
          count += 1
        end
      rescue => e
        logger.warn("jetstream: error while reading bucket", bucket: bucket_name, error: e.message)
        next
      end

      # Атомарно подменяем карту бакета
      @kv_cache[bucket_name] = new_map
      logger.debug("jetstream: cache refreshed", bucket: bucket_name, keys: count)
    end
  rescue => e
    logger.error("jetstream: cache refresh failed", error: e.message, backtrace: e.backtrace)
  end

  # ---------- Обработка requests ----------

def process_requests(event)
  return if @requests.nil?

  requests = event.get(@requests)
  return unless requests.is_a?(Hash)

  requests.each do |bucket_name, config|
    next unless config.is_a?(Hash)

    keys   = config['keys']
    target = config['target']
    append = config['append']
    append = true if append.nil?

    next unless keys.is_a?(Array) && keys.any?
    next if target.nil?

    map = @kv_cache[bucket_name]
    next unless map

    collected = []
    keys.each do |key|
      value = map[key]
      next if value.nil?
      value = [value] unless value.is_a?(Array)
      collected.concat(value)
    end
    next if collected.empty?

    if append
      current = event.get(target) || []
      current = [current] unless current.is_a?(Array)
      event.set(target, (current + collected).uniq)
    else
      event.set(target, collected.uniq)
    end
  end
end

  # ---------- Оригинальные get/set (оставлены для совместимости) ----------

  def do_get(event)
    return false unless @get&.any?
    return false if @bucket.nil? || @bucket.empty?

    begin
      c ||= @jetstream.key_value(bucket)
    rescue => e
      if e.message.include?("bucket not found")
        logger.debug("jetstream:get failed: bucket '#{bucket}' not found")
      else
        logger.error("jetstream:get failed: unexpected error", error: e.message)
      end
      return false
    end

    cache_hits = 0
    begin
      @get.each do |jetstream_key_template, event_field|
        jetstream_key = event.sprintf(jetstream_key_template)
        next if jetstream_key.nil?

        jetstream_key = [jetstream_key].flatten
        jetstream_key.each do |k|
          value = nil
          begin
            value = c.get(k)
            if value
              cache_hits += 1
              update_event_field(event, event_field, value[:value])
            end
          rescue => e
            logger.debug("jetstream:get error", context(key: k, error: e.message))
          end
        end
      end
      return cache_hits > 0
    rescue => e
      logger.debug("cannot get jetstream key", message: e.message)
      return false
    end
  end

  def update_event_field(event, event_field, value)
    field = []
    if event.get(event_field).nil?
      field = Array(event.get(event_field))
    end

    loaded_value = parse_value(value)
    res = field.concat(loaded_value).uniq

    event.set(event_field, res)
  end

  def parse_value(value)
    loaded_value = [value]
    parsed_value = Array(LogStash::Json.load(value))
    loaded_value = parsed_value.is_a?(Array) ? parsed_value : [parsed_value]
    loaded_value
  rescue => e
    logger.trace("failed to parse value", context(value: value, error: e.message))
    value
  end

  def do_set(event)
    return false unless @set&.any?
    return false if @bucket.nil? || @bucket.empty?

    values_by_jetstream_key = @set.each_with_object({}) do |(event_field, jetstream_key_template), memo|
      value = Array(event.get(event_field))
      jetstream_key = event.sprintf(jetstream_key_template)
      jetstream_key = [jetstream_key].flatten.uniq
      jetstream_key.each do |k|
        memo[k] = value unless value.empty?
      end
    end

    return false if values_by_jetstream_key.empty?

    values_by_jetstream_key.each do |jetstream_key, value|
      res = [value]
      old_value = nil
      begin
        old_value = cache.get(jetstream_key)
        if old_value
          old_value = parse_value(old_value[:value])
          res.concat(old_value)
        end
      rescue => e
        if e.message.include?("bucket not found")
          @jetstream.create_key_value(
            name: bucket,
            description: "Auto-created by ngr-logstash-filter-jetstream plugin",
            subjects: ["js.#{bucket}.>"],
            ttl: 0,
            history: 1,
            replicas: 1)
          retry
        else
          logger.trace("jetstream:get miss", context(key: jetstream_key))
        end
      end
      res = res.flatten.uniq
      res = res[0] if res.length == 1

      cache.put(jetstream_key, LogStash::Json.dump(res))
    end

    true
  rescue => e
    logger.debug("cannot set jetstream key", message: e.message)
    false
  end

  # ---------- Соединение ----------

  def new_connection(hosts, options)
    logger.debug('Connecting to Jetstream', context(hosts: mask_passwords_in_urls(hosts), bucket: bucket))
    connect = { :servers => hosts }
    if options[:tls]
      connect[:tls] = { context: options[:tls] }
    end

    nc = NATS.connect(connect)
    @jetstream = nc.jetstream
    nc                # <-- явный возврат соединения
  end

  def reconnect(hosts, options)
    @nc = new_connection(hosts, options)
    @connected.make_true
  rescue => e
    logger.error("Failed to reconnect to Jetstream",
                 hosts: mask_passwords_in_urls(hosts),
                 options: options,
                 message: e.message)
    @connected.make_false
  end

  def connection_available?
    return true if @connected.true?
    return false if @connection_mutex.nil?

    @connection_mutex.synchronize do
      @connected.true? || reconnect(@jetstream_hosts, @jetstream_options)
    end
  end

  def setup_client_tls
    return nil unless @tls_enabled

    tls_context = OpenSSL::SSL::SSLContext.new
    tls_context.ssl_version = @tls_version
    tls_context.verify_mode =
      @tls_verification_mode == "none" ? OpenSSL::SSL::VERIFY_NONE : OpenSSL::SSL::VERIFY_PEER

    if @tls_certificate
      tls_context.cert_store = OpenSSL::X509::Store.new
      ca_file = File.read(@tls_certificate)
      tls_context.cert_store.add_cert(OpenSSL::X509::Certificate.new(ca_file))
    end

    tls_context
  end

  def validate_connection_options
    { :tls => setup_client_tls }
  end

  def validate_connection_hosts
    raise(LogStash::ConfigurationError, "'hosts' cannot be empty") if @hosts.empty?
    @hosts.map(&:to_s)
  end

  def handle_unexpected_error(event, error)
    event.tag(@tag_on_failure)
    logger.error("Unexpected error", message: error.message, backtrace: error.backtrace)
  end

  def context(hash = {})
    @plugin_context ||= { bucket: @bucket }.compact
    @plugin_context.merge(hash)
  end
end