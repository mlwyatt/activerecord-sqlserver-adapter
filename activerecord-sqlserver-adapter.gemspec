<<<<<<< HEAD
# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'activerecord/sqlserver/adapter/version'

Gem::Specification.new do |spec|
  spec.name          = "activerecord-sqlserver-adapter"
  spec.version       = Activerecord::Sqlserver::Adapter::VERSION
  spec.authors       = ["Marcus Wyatt"]
  spec.email         = ["mlwyatt2008@gmail.com"]
  spec.summary       = %q{TODO: Write a short summary. Required.}
  spec.description   = %q{TODO: Write a longer description. Optional.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
=======
# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "active_record/connection_adapters/sqlserver/version"

Gem::Specification.new do |spec|
  spec.name          = 'activerecord-sqlserver-adapter'
  spec.version       = ActiveRecord::ConnectionAdapters::SQLServer::Version::VERSION
  spec.platform      = Gem::Platform::RUBY
  spec.license       = 'MIT'
  spec.authors       = ['Ken Collins', 'Anna Carey', 'Will Bond', 'Murray Steele', 'Shawn Balestracci', 'Joe Rafaniello', 'Tom Ward']
  spec.email         = ['ken@metaskills.net', 'will@wbond.net']
  spec.homepage      = 'http://github.com/rails-sqlserver/activerecord-sqlserver-adapter'
  spec.summary       = 'ActiveRecord SQL Server Adapter.'
  spec.description   = 'ActiveRecord SQL Server Adapter. SQL Server 2012 and upward.'
  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']
  spec.add_dependency 'activerecord', '~> 4.2.1'
>>>>>>> 39c26d444de3cb7923c36674683f086480c11ec0
end
