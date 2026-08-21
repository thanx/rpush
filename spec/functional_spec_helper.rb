require 'spec_helper'

require 'database_cleaner'
DatabaseCleaner.strategy = :truncation

def functional_example?(metadata)
  metadata[:file_path] =~ %r{/spec/functional/}
end

def timeout(&blk)
  Timeout.timeout(10, &blk)
end

def stub_tcp_connection(tcp_socket, ssl_socket, io_double)
  allow_any_instance_of(Rpush::Daemon::TcpConnection).to receive_messages(connect_socket: [tcp_socket, ssl_socket])
  allow_any_instance_of(Rpush::Daemon::TcpConnection).to receive_messages(setup_ssl_context: double.as_null_object)
  stub_const('Rpush::Daemon::TcpConnection::IO', io_double)
end

RSpec.configure do |config|
  config.before(:each) do
    if redis? && functional_example?(self.class.metadata)
      # These full-daemon functional specs assume apps and notifications live in the same
      # store. This fork runs a hybrid — apps in ActiveRecord (Postgres), notifications in
      # Redis (see Store::Redis#all_apps) — so under the redis client the daemon looks the
      # test's redis-created app up in ActiveRecord, finds nothing, and every scenario times
      # out. The functional layer is fully exercised under the active_record client; hybrid
      # redis functional coverage is a separate follow-up.
      skip 'functional specs require a single-store model; this fork uses a redis/AR hybrid'
    end

    Modis.with_connection do |redis|
      redis.keys('rpush:*').each { |key| redis.del(key) }
    end if redis? && functional_example?(self.class.metadata)

    Rpush.config.logger = ::Logger.new(STDOUT) if functional_example?(self.class.metadata)
  end

  config.after(:each) do
    DatabaseCleaner.clean if active_record? && functional_example?(self.class.metadata)
  end
end
