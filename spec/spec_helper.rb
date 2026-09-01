ENV['RAILS_ENV'] = 'test'
def client
  (ENV['CLIENT'] || :active_record).to_sym
end

if !ENV['CI'] || (ENV['CI'] && ENV['QUALITY'] == 'true')
  begin
    require './spec/support/simplecov_helper'
    include SimpleCovHelper
    start_simple_cov("rpush-#{client}-#{RUBY_VERSION}")
  rescue LoadError
    puts "Coverage disabled."
  end
end

require 'debug'
require 'timecop'
require 'activerecord-jdbc-adapter' if defined? JRUBY_VERSION

require 'rpush'
require 'rpush/daemon'
require 'rpush/client/redis'
require 'rpush/client/active_record'
require 'rpush/daemon/store/active_record'
require 'rpush/daemon/store/redis'

def active_record?
  client == :active_record
end

def redis?
  client == :redis
end

# The ActiveRecord schema is needed under both clients: Store::Redis#all_apps reads apps from
# ActiveRecord (this fork's hybrid — apps in the RDBMS, notifications in the per-client store),
# so even the redis client needs the apps table.
require 'support/active_record_setup'

RPUSH_ROOT = '/tmp/rails_root'

Rpush.configure do |config|
  config.client = client
  config.log_level = ::Logger::Severity::DEBUG
end

RPUSH_CLIENT = Rpush.config.client

# Under the redis client, keep the ActiveRecord app store in sync with the redis one, so the
# daemon (which reads apps from ActiveRecord) sees the apps the specs create in Redis.
require 'support/redis_app_mirror' if redis?

path = File.join(File.dirname(__FILE__), 'support')
TEST_CERT = File.read(File.join(path, 'cert_without_password.pem'))
TEST_CERT_WITH_PASSWORD = File.read(File.join(path, 'cert_with_password.pem'))

VAPID_KEYPAIR = WebPush.generate_key.to_hash.merge(subject: 'rpush-test@example.org').to_json

def after_example_cleanup
  Rpush.logger = nil
  Rpush::Daemon.store = nil
  Rpush::Deprecation.muted do
    Rpush.config = nil
    Rpush.config.client = RPUSH_CLIENT
  end
  Rpush.plugins.values.each(&:unload)
  Rpush.instance_variable_set('@plugins', {})
end

RSpec.configure do |config|
  config.before(:each) do
    Rpush.config.log_file = File.join(RPUSH_ROOT, 'rpush.log')
    allow(Rpush).to receive(:root) { RPUSH_ROOT }
  end

  config.after(:each) do
    after_example_cleanup
  end
end
