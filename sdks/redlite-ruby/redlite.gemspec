# frozen_string_literal: true

require_relative "lib/redlite/version"

Gem::Specification.new do |spec|
  spec.name = "redlite"
  spec.version = Redlite::VERSION
  spec.authors = ["Redlite Contributors"]
  spec.email = [""]

  spec.summary = "Redis-compatible embedded database with SQLite durability"
  spec.description = "Redlite is a Redis-compatible embedded database built in Rust with SQLite durability. This gem provides Ruby bindings via FFI."
  spec.homepage = "https://github.com/redlite-db/redlite"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github])
    end
  end
  spec.require_paths = ["lib"]

  spec.add_dependency "ffi", "~> 1.15"

  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.12"
end
