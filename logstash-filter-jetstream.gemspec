Gem::Specification.new do |s|
    s.name          = 'logstash-filter-jetstream'
    s.version       = '0.1.0'
    s.licenses      = ['Apache-2.0']
    s.summary       = 'A Logstash filter plugin for interacting with NATS Jetstream KV storage'
    s.authors       = ["vaigard"]
    s.email         = 'vaigard03117@gmail.com'
    s.homepage      = "https://gitlab.ngrsoftlab.ru/backend/logstash-filter-jetstream"
    s.require_paths = ['lib']
  
    # Files
    s.files = Dir['lib/**/*','spec/**/*','*.gemspec','*.md','Gemfile']
     # Tests
    s.test_files = s.files.grep(%r{^(test|spec|features)/})
  
    # Special flag to let us know this is actually a logstash plugin
    s.metadata = { "logstash_plugin" => "true", "logstash_group" => "filter" }
  
    # Gem dependencies
    s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
    s.add_runtime_dependency "nats-pure", "~> 2.4"
    s.add_development_dependency 'logstash-devutils'
  end
  