# Logstash Jetstream Plugin

This plugin enables key/value lookup enrichment against a NATS Jetstream storage.
You can use this plugin to query for a value, and set it if not found.

# Usage

    jetstream {
      hosts => ["nats://alertix:alertix@alertixdev.ngrsoftlab.ru:4222"]
      bucket => "test_tags"
      get => {
        "test_tag" => "[cache_tag]"
      }
      add_tag => ["from_cache"]
      id => "jetstream-get"
      tls_certificate => "/usr/share/logstash/config/cas.crt"
      tls_enabled => true
    }


    jetstream {
      hosts => ["nats://alertix:alertix@alertixdev.ngrsoftlab.ru:4222"]
      bucket => "test_tags"
      set => {
        "[host][address]" => "%{[host][address]}"
      }
      add_tag => ["to_cache"]
      id => "jetstream-set"
      tls_certificate => "/usr/share/logstash/config/cas.crt"
      tls_enabled => true
    }

## Plugin options
* `hosts` (default: ["nats://localhost:4222"]) - list of addresses to connect, may contains login and passwords
* `bucket` - name of Jetstream bucket, required
* `get` - object with mapping of jetstream keys to event fields, optional
* `set` - object with mapping of event fields to jetstream keys, optional
* `tls_certificate` - certificate of server, optional
* `tls_enabled` (default: false) - is need for certificate validation
* `tls_version` (default: TLSv1.2, one of [TLSv1.1, TLSv1.2, TLSv1.3]) * minimal available version TLS
* `tls_verification_mode` (default: full, one of [full, none]) - is need for host and certificate verification
* `tag_on_failure` (default: _jetstream_failure) - tag on failure


# Installation

The easiest way to use this plugin is by installing it through rubygems like any other logstash plugin. To get the latest versio installed, you should run the following command: `bin/logstash-plugin install logstash-filter-jetstream`

# Building the gem and installing a local version

To build the gem yourself, use `gem build logstash-filter-jetstream.gemspec` in the root of this repository. Alternatively, you can download a built version of the gem from the `dist` branch of this repository.

To install, run the following command, assuming the gem is in the local directory: `$LOGSTASH_HOME/bin/plugin install logstash-filter-jetstream-X.Y.Z.gem`
