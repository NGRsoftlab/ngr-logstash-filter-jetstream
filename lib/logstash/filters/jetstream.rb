# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/json"
require "openssl"
require 'nats/client'

class LogStash::Filters::Jetstream < LogStash::Filters::Base
  config_name "jetstream"

  config :hosts, :validate => :array, :default => ["nats://localhost:4222"]
  config :bucket, :validate => :string, :required => true
  config :get, :validate => :hash, :required => false
  config :set, :validate => :hash, :required => false
  config :tls_certificate, :validate => :path
  config :tls_enabled, :validate => :boolean, :default => false
  config :tls_version, :validate => %w[TLSv1.1 TLSv1.2 TLSv1.3], :default => 'TLSv1.2'
  config :tls_verification_mode, :validate => %w[full none], :default => 'full'
  config :tag_on_failure, :validate => :string, :default => "_jetstream_failure"

  attr_reader :cache

  def register
    @connection_mutex = Mutex.new
    @jetstream_hosts = validate_connection_hosts
    @jetstream_options = validate_connection_options
    @cache = new_connection(@jetstream_hosts, @jetstream_options)
    @connected = Concurrent::AtomicBoolean.new(true)
  rescue => e
    logger.error("Failed to connect to Jetstream", hosts: @jetstream_hosts, options: @jetstream_options, message: e.message)
    @connected = Concurrent::AtomicBoolean.new(false)
  end

  def filter(event)
    unless connection_available?
      event.tag(@tag_on_failure)
      return
    end

    begin
      set_success = do_set(event)
      get_success = do_get(event)
      filter_matched(event) if set_success || get_success
    rescue => e
      handle_unexpected_error(event, e)
    end
  end

  def close
    @connection_mutex.synchronize do
      @connected.make_false
      cache.close
    end
  rescue => e
    logger.debug("Error closing Jetstream connection", message: e.message)
  end

  private

  def do_get(event)
    return false unless @get&.any?

    cache_hits = 0
    @get.each do |jetstream_key_template, event_field|
      jetstream_key = event.sprintf(jetstream_key_template)
      next if jetstream_key.nil?

      jetstream_key = [jetstream_key].flatten
      jetstream_key.each do |k|
        value = nil
        begin
          value = cache.get(k)
          if value
            logger.trace("jetstream:get hit", context(key: k, value: value[:value]))
            cache_hits += 1
            update_event_field(event, event_field, value[:value])
          else
            logger.trace("jetstream:get miss", context(key: k))
          end
        rescue => e
          logger.trace("jetstream:get miss", context(key: k))
        end
      end
    end

    return cache_hits > 0
  rescue => e
    logger.debug("cannot get jetstream key", message: e.message)
    return false
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
    return loaded_value
  rescue => e
    logger.trace("failed to parse value", context(value: value, error: e.message))
    return value
  end

  def do_set(event)
    return false unless @set&.any?

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
        logger.trace("jetstream:get miss", context(key: jetstream_key))
      end
      res = res.flatten.uniq
      res = res[0] if res.length == 1

      cache.put(jetstream_key, LogStash::Json.dump(res))
    end

    return true
  rescue => e
    logger.debug("cannot set jetstream key", message: e.message)
    return false
  end

  def new_connection(hosts, options)
    logger.debug('Connecting to Jetstream', context(hosts: hosts, bucket: bucket))
    connect = { :servers => hosts }
    if options[:tls]
      connect[:tls] = {context: options[:tls]}
    end

    nc = NATS.connect(connect)
    js = nc.jetstream
    js.key_value(options[:bucket])
  end

  def reconnect(hosts, options)
    @cache = new_connection(hosts, options)
    @connected.make_true
  rescue => e
    logger.error("Failed to reconnect to Jetstream", hosts: hosts, options: options, message: e.message)
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

    tls_context.verify_mode = "none" ? OpenSSL::SSL::VERIFY_NONE : OpenSSL::SSL::VERIFY_PEER

    if @tls_certificate
      tls_context.cert_store = OpenSSL::X509::Store.new
      ca_file = File.read(@tls_certificate)
      tls_context.cert_store.add_cert(OpenSSL::X509::Certificate.new(ca_file))
    end

    tls_context
  end

  def validate_connection_options
    { :tls => setup_client_tls, :bucket => @bucket }
  end

  def validate_connection_hosts
    raise(LogStash::ConfigurationError, "'hosts' cannot be empty") if @hosts.empty?
    @hosts.map(&:to_s)
  end

  def handle_communication_error(event, error)
    event.tag(@tag_on_failure)
    logger.error("Jetstream communication error", hosts: @jetstream_hosts, options: @jetstream_options, message: error.message)
    close
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
