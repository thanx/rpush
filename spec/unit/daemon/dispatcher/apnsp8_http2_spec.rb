require 'unit_spec_helper'

describe Rpush::Daemon::Dispatcher::Apnsp8Http2 do
  let(:app) { double(name: 'my_app', environment: 'production') }
  let(:delivery_class) { double('DeliveryClass') }
  let(:logger) { double('logger').as_null_object }
  let(:client) { double('NetHttp2::Client') }

  subject(:dispatcher) { described_class.new(app, delivery_class) }

  before do
    allow(Rpush).to receive_messages(logger: logger)
    allow(Rpush::Daemon::Apnsp8::Token).to receive(:new).and_return(double('Token'))
    @callbacks = {}
    allow(NetHttp2::Client).to receive(:new).and_return(client)
    allow(client).to receive(:on) { |event, &blk| @callbacks[event] = blk }
  end

  describe 'the client error callback' do
    it 'logs a structured connection_error event when the connection raises a socket error' do
      dispatcher
      allow(dispatcher).to receive(:reflect)
      expect(logger).to receive(:error)
        .with('event=connection_error app=my_app error=Errno::ECONNRESET')
      @callbacks[:error].call(Errno::ECONNRESET.new('Connection reset by peer'))
    end

    it 'still reflects the error so upstream handlers fire' do
      dispatcher
      expect(dispatcher).to receive(:reflect).with(:error, kind_of(Errno::ECONNRESET))
      @callbacks[:error].call(Errno::ECONNRESET.new('Connection reset by peer'))
    end
  end
end
