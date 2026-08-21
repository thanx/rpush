require 'unit_spec_helper'

describe Rpush::Daemon::Dispatcher::ApnsHttp2 do
  let(:app) { double(name: 'my_app', environment: 'production', certificate: 'cert', password: 'pass') }
  let(:delivery_class) { double('DeliveryClass') }
  let(:logger) { double('logger').as_null_object }
  let(:client) { double('NetHttp2::Client') }
  let(:ssl_context) { double('OpenSSL::SSL::SSLContext') }

  subject(:dispatcher) { described_class.new(app, delivery_class) }

  before do
    allow(Rpush).to receive_messages(logger: logger)
    allow_any_instance_of(described_class).to receive(:prepare_ssl_context).and_return(ssl_context)
    @callbacks = {}
    allow(NetHttp2::Client).to receive(:new).and_return(client)
    allow(client).to receive(:on) { |event, &blk| @callbacks[event] = blk }
  end

  describe 'the client error callback' do
    it 'logs a structured connection_error event including the error message, not just its class' do
      dispatcher
      allow(dispatcher).to receive(:reflect)
      expect(logger).to receive(:error)
        .with('event=connection_error app=my_app error="Errno::ECONNRESET: Connection reset by peer"')
      @callbacks[:error].call(Errno::ECONNRESET.new)
    end

    it 'still reflects the error so upstream handlers fire' do
      dispatcher
      expect(dispatcher).to receive(:reflect).with(:error, kind_of(Errno::ECONNRESET))
      @callbacks[:error].call(Errno::ECONNRESET.new('Connection reset by peer'))
    end
  end
end
