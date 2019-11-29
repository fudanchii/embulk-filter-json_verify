
Gem::Specification.new do |spec|
  spec.name          = "embulk-filter-json_verify"
  spec.version       = "0.1.0"
  spec.authors       = ["Nurahmadie"]
  spec.summary       = "Json Verify filter plugin for Embulk"
  spec.description   = "Json Verify"
  spec.email         = ["nurahmadie@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/nurahmadie/embulk-filter-json_verify"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'embulk', ['>= 0.8.15']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'pry-debugger-jruby', ['>= 0']
end
