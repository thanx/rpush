require 'unit_spec_helper'

describe Rpush::Daemon::DispatcherLoop do
  def run_dispatcher_loop
    dispatcher_loop.start
    dispatcher_loop.stop
  end

  let(:notification) { double }
  let(:batch) { double(notification_processed: nil) }
  let(:queue) { Queue.new }
  let(:dispatcher) { double(dispatch: nil, cleanup: nil) }
  let(:dispatcher_loop) { Rpush::Daemon::DispatcherLoop.new(queue, dispatcher) }
  let(:store) { double(Rpush::Daemon::Store::ActiveRecord, release_connection: nil) }

  before do
    allow(Rpush::Daemon).to receive_messages(store: store)
    queue.push([notification, batch])
  end

  it 'logs errors' do
    logger = double
    allow(Rpush).to receive_messages(logger: logger)
    error = StandardError.new
    allow(dispatcher).to receive(:dispatch).and_raise(error)
    expect(Rpush.logger).to receive(:error).with(error)
    run_dispatcher_loop
  end

  it 'reflects an exception' do
    allow(Rpush).to receive_messages(logger: double(error: nil))
    error = StandardError.new
    allow(dispatcher).to receive(:dispatch).and_raise(error)
    expect(dispatcher_loop).to receive(:reflect).with(:error, error)
    run_dispatcher_loop
  end

  describe 'sandbox simulated delivery' do
    before do
      queue.clear
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with('RAILS_ENV').and_return('sandbox')
      allow(ENV).to receive(:[]).with('RPUSH_SIMULATE_DELIVERY').and_return('true')
    end

    it 'resolves a single notification without calling the provider dispatcher' do
      queue.push(Rpush::Daemon::QueuePayload.new(batch, notification))
      expect(batch).to receive(:mark_simulated).with(notification)
      expect(batch).to receive(:notification_processed)
      expect(dispatcher).not_to receive(:dispatch)
      run_dispatcher_loop
    end

    it 'resolves an APNs batch without calling the provider dispatcher' do
      queue.push(Rpush::Daemon::QueuePayload.new(batch))
      expect(batch).to receive(:mark_all_simulated)
      expect(batch).to receive(:all_processed)
      expect(dispatcher).not_to receive(:dispatch)
      run_dispatcher_loop
    end

    it 'does not simulate outside sandbox' do
      allow(ENV).to receive(:[]).with('RAILS_ENV').and_return('production')
      queue.push(Rpush::Daemon::QueuePayload.new(batch, notification))
      expect(dispatcher).to receive(:dispatch)
      run_dispatcher_loop
    end
  end

  describe 'sandbox synthetic probe records' do
    before do
      queue.clear
      allow(ENV).to receive(:[]).and_call_original
      allow(ENV).to receive(:[]).with('RAILS_ENV').and_return('sandbox')
      allow(ENV).to receive(:[]).with('RPUSH_SIMULATE_DELIVERY').and_return(nil)
      allow(notification).to receive_messages(data: {}, device_token: 'real-device-token')
    end

    it 'never dispatches a marked synthetic notification to FCM' do
      allow(notification).to receive_messages(
        data: { 'pla1859_run' => 'probe1' }, device_token: 'pla1859-probe1-0'
      )
      queue.push(Rpush::Daemon::QueuePayload.new(batch, notification))
      expect(batch).to receive(:mark_simulated).with(notification)
      expect(batch).to receive(:notification_processed)
      expect(dispatcher).not_to receive(:dispatch)
      run_dispatcher_loop
    end

    it 'continues to dispatch ordinary sandbox notifications' do
      queue.push(Rpush::Daemon::QueuePayload.new(batch, notification))
      expect(dispatcher).to receive(:dispatch)
      run_dispatcher_loop
    end

    it 'does not simulate marked notifications outside sandbox' do
      allow(ENV).to receive(:[]).with('RAILS_ENV').and_return('production')
      allow(notification).to receive_messages(
        data: { 'pla1859_run' => 'probe1' }, device_token: 'pla1859-probe1-0'
      )
      queue.push(Rpush::Daemon::QueuePayload.new(batch, notification))
      expect(dispatcher).to receive(:dispatch)
      run_dispatcher_loop
    end
  end

  describe 'stop' do
    before do
      queue.clear
    end

    it 'instructs the dispatcher to cleanup' do
      expect(dispatcher).to receive(:cleanup)
      run_dispatcher_loop
    end

    it 'releases the store connection' do
      expect(Rpush::Daemon.store).to receive(:release_connection)
      run_dispatcher_loop
    end
  end
end
