# Logstash Jetstream Plugin

This plugin enables key/value lookup enrichment against a NATS Jetstream storage.
You can use this plugin to query for a value, and set it if not found.

## Features

- **Get operations**: Retrieve data from KV buckets and store in events
- **Set operations**: Save data from events to KV buckets
- **Batch requests**: Execute multiple get requests via metadata
- **TLS support**: Secure connection to NATS server
- **Auto-create buckets**: Automatic bucket creation when needed

# Usage

## Basic Get Operations

Retrieve data from a bucket and store it in an event field:

    jetstream {
      hosts => ["nats://logstash:logstash@server.domain.ru:4222"]
      bucket => "test_tags"
      get => {
        "test_tag" => "[cache_tag]"
      }
      add_tag => ["from_cache"]
      id => "jetstream-get"
      tls_certificate => "/usr/share/logstash/config/cas.crt"
      tls_enabled => true
    }

## Basic Set Operations

Save data from an event to a bucket

    jetstream {
      hosts => ["nats://logstash:logstash@server.domain.ru:4222"]
      bucket => "test_tags"
      set => {
        "[host][address]" => "%{[host][address]}"
      }
      add_tag => ["to_cache"]
      id => "jetstream-set"
      tls_certificate => "/usr/share/logstash/config/cas.crt"
      tls_enabled => true
    }

## Batch Requests via requests Parameter

Dynamically execute multiple requests to different buckets

    filter {
      # Generate requests via Ruby code
      ruby {
        code => '
          requests = []

        # Requests to different buckets
        requests << {
          "bucket" => "ips_bucket",
          "key" => event.get("[source][ip]"),
          "target" => "[tags]",
          "append" => true
        }

        requests << {
          "bucket" => "userenames_bucket",
          "key" => event.get("[user][name]"),
          "target" => "[tags]",
          "append" => true
        }

        event.set("[@metadata][custom_requests]", requests)
      '
    }

    # Execute requests
    jetstream {
        hosts => ["nats://logstash:logstash@server.domain.ru:4222"]
        bucket => "my_bucket"  # an existing bucket to avoid errors
        tls_certificate => "/usr/share/logstash/config/cas.crt"
        tls_enabled => true
        tls_verification_mode => "none"
        tag_on_failure => "_jetstream_failure"
        requests => "[@metadata][jetstream_requests]"
    }
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
* `requests` (default: nil) - variable for storing requests to broker


# Installation

The easiest way to use this plugin is by installing it through rubygems like any other logstash plugin. To get the latest versio installed, you should run the following command: `bin/logstash-plugin install logstash-filter-jetstream`

# Building the gem and installing a local version

To build the gem yourself, use `gem build logstash-filter-jetstream.gemspec` in the root of this repository. Alternatively, you can download a built version of the gem from the `dist` branch of this repository.

To install, run the following command, assuming the gem is in the local directory: `$LOGSTASH_HOME/bin/plugin install logstash-filter-jetstream-X.Y.Z.gem`
