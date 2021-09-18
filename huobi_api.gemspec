# frozen_string_literal: true

require_relative "lib/huobi_api/version"

Gem::Specification.new do |spec|
  spec.name          = "huobi_api"
  spec.version       = HuobiApi::VERSION
  spec.authors       = ["malongshuai"]
  spec.email         = ["mlongshuai@gmail.com"]

  spec.summary       = "HuobiApi"
  spec.description   = "HuobiApi"
  spec.homepage      = "https://www.junmajinlong.com"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 3.0.1")

  # spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"

  spec.metadata["homepage_uri"] = spec.homepage
  # spec.metadata["source_code_uri"] = "TODO: Put your gem's public repo URL here."
  # spec.metadata["changelog_uri"] = "TODO: Put your gem's CHANGELOG.md URL here."

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  # spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  # spec.executables << 'get_klines'
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  spec.add_dependency "faye-websocket", "~> 0.11.0"
  spec.add_dependency "async"
  spec.add_dependency "async-io"
  spec.add_dependency "httplog"
  spec.add_dependency "oj", "~> 3.13.0"
  spec.add_dependency "multi_json"

  # For more information and examples about making a new gem, checkout our
  # guide at: https://bundler.io/guides/creating_gem.html
end
