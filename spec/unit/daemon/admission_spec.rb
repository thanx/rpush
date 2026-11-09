require 'unit_spec_helper'
require 'delegate'

describe Rpush::Daemon::Admission do
  let(:store) do
    double('store', mark_failed: nil, mark_delivered: nil, mark_retryable: nil, mark_batch_delivered: nil,
      mark_batch_failed: nil, mark_batch_retryable: nil)
  end
  let(:logger) { double('logger').as_null_object }
  let(:notification) { double('Notification', id: 7, fail_after: nil, delivered: false, failed: false) }
  let(:other) { double('Other notification', id: 8, fail_after: nil, delivered: false, failed: false) }
  let(:batch) { Rpush::Daemon::Batch.new([notification, other]) }
  let(:reflections) { [] }

  before do
    allow(Rpush::Daemon).to receive_messages(store: store)
    allow(Rpush).to receive_messages(logger: logger)
    allow(Rpush::Daemon::Admission).to receive(:reflect) { |*args| reflections << args }
  end

  def admit(hook)
    Rpush.config.admission = hook
    described_class.admit(notification, batch)
  end

  it 'admits without changing outcomes when no hook exists' do
    expect(described_class.admit(notification, batch)).to be(true)
    expect(batch.failed).to be_empty
    expect(batch.refused).to be_empty
    expect(reflections).to be_empty
  end

  it 'admits only literal true' do
    seen = []
    expect(admit(->(n) { seen << n; true })).to be(true)
    expect(seen).to eq([notification])
    expect(batch.refused).to be_empty
  end

  it 'records a terminal named refusal and its reflection' do
    expect(admit(->(_) { :contact_blocked })).to be(false)
    expect(batch.refused?(notification)).to be(true)
    expect(batch.failed).to eq([nil, 'Refused before send: contact_blocked'] => [notification])
    expect(store).to have_received(:mark_failed)
      .with(notification, nil, 'Refused before send: contact_blocked', kind_of(Time), persist: false)
    expect(reflections).to eq([[:notification_refused, notification, 'contact_blocked']])
  end

  [nil, false, 'yes', 1].each do |value|
    it "refuses a non-true result (#{value.inspect})" do
      expect(admit(->(_) { value })).to be(false)
      expected = value.is_a?(String) ? value : 'refused'
      expect(batch.failed.keys).to eq([[nil, "Refused before send: #{expected}"]])
    end
  end

  it 'refuses an object that delegates equality to true' do
    expect(admit(->(_) { SimpleDelegator.new(true) })).to be(false)
    expect(batch.refused?(notification)).to be(true)
  end

  it 'refuses a hook error without allowing provider IO' do
    error = RuntimeError.new('database unavailable')
    expect(admit(->(_) { raise error })).to be(false)
    expect(batch.failed.keys).to eq([[nil, 'Refused before send: admission_error']])
    expect(reflections).to eq([[:error, error], [:notification_refused, notification, 'admission_error']])
  end

  describe 'batch outcomes after refusal' do
    before { admit(->(n) { n.equal?(other) || :refused_here }) }

    it 'does not mark a refused notification delivered' do
      batch.mark_all_delivered
      expect(batch.delivered).to eq([other])
    end

    it 'does not replace refusal with a transport failure' do
      batch.mark_all_failed(nil, 'connection reset')
      expect(batch.failed).to eq([nil, 'Refused before send: refused_here'] => [notification],
        [nil, 'connection reset'] => [other])
    end

    it 'does not include a refused notification in the unresolved stream sweep' do
      expect(batch.unresolved).to eq([other])
    end
  end

  describe Rpush::Daemon::Dispatcher::Http do
    let(:app) { double('app') }
    let(:delivery_class) { double('delivery class') }
    let(:http) { double('http') }
    let(:dispatcher) { described_class.new(app, delivery_class) }
    let(:counting_batch) { double('batch', notification_processed: nil, mark_refused: nil) }

    before { allow(Net::HTTP::Persistent).to receive_messages(new: http) }

    it 'does not construct a delivery for a refused notification' do
      Rpush.config.admission = ->(_) { :membership_retired }
      expect(delivery_class).not_to receive(:new)
      dispatcher.dispatch(Rpush::Daemon::QueuePayload.new(counting_batch, notification))
      expect(counting_batch).to have_received(:notification_processed).once
    end

    it 'constructs a delivery for an admitted notification' do
      Rpush.config.admission = ->(_) { true }
      delivery = double('delivery', perform: nil)
      expect(delivery_class).to receive(:new).with(app, http, notification, counting_batch).and_return(delivery)
      dispatcher.dispatch(Rpush::Daemon::QueuePayload.new(counting_batch, notification))
      expect(counting_batch).not_to have_received(:notification_processed)
    end
  end

  describe Rpush::Daemon::Apns2::Delivery do
    let(:app) { double(bundle_id: 'com.example.app', name: 'admission_app') }
    let(:http_request) { double(on: nil) }
    let(:client) { double(call_async: nil, join: nil, prepare_request: http_request) }
    let(:apns_one) { Rpush::Apns2::Notification.new(device_token: 'a' * 64, alert: 'one') }
    let(:apns_two) { Rpush::Apns2::Notification.new(device_token: 'b' * 64, alert: 'two') }
    let(:apns_batch) { Rpush::Daemon::Batch.new([apns_one, apns_two]) }

    it 'creates a provider stream only for an admitted notification' do
      Rpush.config.admission = ->(n) { n.equal?(apns_two) || :generation_changed }
      allow(apns_batch).to receive(:unresolved).and_return([])
      described_class.new(app, client, apns_batch).perform
      expect(client).to have_received(:prepare_request).once
        .with(:post, "/3/device/#{'b' * 64}", hash_including(:body, :headers))
      expect(client).to have_received(:call_async).once
      expect(apns_batch.refused).to eq([apns_one])
    end
  end

  describe Rpush::Daemon::Apnsp8::Delivery do
    let(:app) { double(bundle_id: 'com.example.app', name: 'token_admission_app') }
    let(:client) { double(prepare_request: nil, call_async: nil, join: nil) }
    let(:token_provider) { double('token provider') }
    let(:apns_notification) { Rpush::Apns2::Notification.new(device_token: 'c' * 64, alert: 'one') }
    let(:apns_batch) { Rpush::Daemon::Batch.new([apns_notification]) }

    it 'refuses before it constructs or sends an authenticated provider request' do
      Rpush.config.admission = ->(_) { :privacy_epoch_mismatch }
      described_class.new(app, client, token_provider, apns_batch).perform

      expect(client).not_to have_received(:prepare_request)
      expect(client).not_to have_received(:call_async)
      expect(apns_batch.failed).to eq(
        [nil, 'Refused before send: privacy_epoch_mismatch'] => [apns_notification]
      )
    end
  end

  describe Rpush::Daemon::Apns::Delivery do
    let(:app) { double(name: 'legacy_app') }
    let(:connection) { double('connection', write: nil) }
    let(:refused_one) { double('refused', id: 1, to_binary: 'REFUSED', fail_after: nil, device_token: 'x') }
    let(:sent_one) { double('sent', id: 2, to_binary: 'SENT', fail_after: nil, device_token: 'y') }
    let(:legacy_batch) { Rpush::Daemon::Batch.new([refused_one, sent_one]) }

    it 'sends only admitted frames and keeps refusal terminal' do
      Rpush.config.admission = ->(n) { n.equal?(sent_one) || :contact_blocked }
      described_class.new(app, connection, legacy_batch).perform
      expect(connection).to have_received(:write).once.with('SENT')
      expect(legacy_batch.delivered).to eq([sent_one])
      expect(legacy_batch.refused).to eq([refused_one])
    end

    it 'sends no frame when all notifications are refused' do
      Rpush.config.admission = ->(_) { :contact_blocked }
      described_class.new(app, connection, legacy_batch).perform
      expect(connection).not_to have_received(:write)
      expect(legacy_batch.delivered).to be_empty
    end
  end
end
