require 'unit_spec_helper'

describe Rpush::Daemon::Apnsp8::Delivery do
  subject(:delivery) { described_class.new(app, http2_client, token_provider, batch) }

  let(:app) { double(bundle_id: 'MY BUNDLE ID', name: 'MY APP') }
  let(:notification1) { double('Notification 1', data: {}, as_json: {}).as_null_object }
  let(:notification2) { double('Notification 2', data: {}, as_json: {}).as_null_object }

  let(:token_provider) { double(token: 'MY JWT TOKEN') }
  let(:max_concurrent_streams) { 100 }
  let(:remote_settings) { { settings_max_concurrent_streams: max_concurrent_streams } }
  let(:http_request) { double(on: nil) }
  let(:http2_client) do
    double(
      stream_count: 0,
      call_async: nil,
      join: nil,
      prepare_request: http_request,
      remote_settings: remote_settings
    )
  end

  let(:batch) do
    double(mark_delivered: nil, mark_retryable: nil, mark_failed: nil,
           all_processed: nil, unresolved: [])
  end
  let(:logger) { double('logger').as_null_object }

  before do
    allow(batch).to receive(:each_notification) do |&blk|
      [notification1, notification2].each(&blk)
    end
    allow(Rpush).to receive_messages(logger: logger)
    allow(delivery).to receive(:reflect)
  end

  describe '#perform' do
    context 'with an HTTP2 client where max concurrent streams is not set' do
      let(:max_concurrent_streams) { 0x7fffffff }

      it 'does not fall into an infinite loop on notifications after the first' do
        start = Time.now
        thread = Thread.new { delivery.perform }

        loop do
          break unless thread.alive?

          if Time.now - start > 1
            thread.kill
            fail 'Stuck in an infinite loop'
          end
        end
      end
    end

    context 'when the connection drops and a stream is left unresolved' do
      before { allow(batch).to receive(:unresolved).and_return([notification1]) }

      it 'marks the abandoned notification retryable rather than discarding it' do
        expect(batch).to receive(:mark_retryable).with(notification1, anything)
        delivery.perform
      end
    end

    context 'when every stream resolved' do
      it 'marks nothing retryable' do
        expect(batch).not_to receive(:mark_retryable)
        delivery.perform
      end
    end
  end

  describe '#handle_response' do
    def handle(response)
      delivery.send(:handle_response, notification1, response)
    end

    context 'with a 200 status' do
      it 'marks the notification delivered' do
        expect(batch).to receive(:mark_delivered).with(notification1)
        handle(code: 200)
      end
    end

    context 'with a retryable status' do
      it 'marks the notification retryable' do
        expect(batch).to receive(:mark_retryable).with(notification1, anything)
        handle(code: 503)
      end
    end

    context 'with no status (stream closed before APNs responded)' do
      it 'marks the notification retryable, not failed' do
        expect(batch).to receive(:mark_retryable).with(notification1, anything)
        handle({})
      end

      it 'does not mark the notification failed' do
        expect(batch).not_to receive(:mark_failed)
        handle({})
      end
    end

    context 'with a permanent error status' do
      it 'marks the notification failed' do
        expect(batch).to receive(:mark_failed).with(notification1, 400, 'BadDeviceToken')
        handle(code: 400, failure_reason: 'BadDeviceToken')
      end
    end
  end
end
