require 'unit_spec_helper'

describe Rpush::Daemon::Store::Redis do
  it_behaves_like 'Rpush::Daemon::Store'

  let(:app) { Rpush::Client::Redis::Apns::App.create!(name: 'my_app', environment: 'development', certificate: TEST_CERT) }
  let(:notification) { Rpush::Client::Redis::Apns::Notification.create!(device_token: "a" * 64, app: app) }
  let(:store) { Rpush::Daemon::Store::Redis.new }
  let(:time) { Time.now.utc }
  let(:logger) { double(Rpush::Logger, error: nil, internal_logger: nil) }

  before do
    allow(Rpush).to receive_messages(logger: logger)
    allow(Time).to receive_messages(now: time)
  end

  describe 'deliverable_notifications' do
    it 'loads notifications in batches' do
      Rpush.config.batch_size = 100
      allow(store).to receive_messages(pending_notification_ids: [1, 2, 3, 4])
      expect(Rpush::Client::Redis::Notification).to receive(:find).exactly(4).times
      store.deliverable_notifications(Rpush.config.batch_size)
    end

    it 'loads an undelivered notification without deliver_after set' do
      notification.update!(delivered: false, deliver_after: nil)
      expect(store.deliverable_notifications(Rpush.config.batch_size)).to eq [notification]
    end

    it 'loads an notification with a deliver_after time in the past' do
      notification.update!(delivered: false, deliver_after: 1.hour.ago)
      expect(store.deliverable_notifications(Rpush.config.batch_size)).to eq [notification]
    end

    it 'does not load an notification with a deliver_after time in the future' do
      notification
      notification = store.deliverable_notifications(Rpush.config.batch_size).first
      store.mark_retryable(notification, 1.hour.from_now)
      expect(store.deliverable_notifications(Rpush.config.batch_size)).to be_empty
    end

    it 'does not load a previously delivered notification' do
      notification
      notification = store.deliverable_notifications(Rpush.config.batch_size).first
      store.mark_delivered(notification, Time.now)
      expect(store.deliverable_notifications(Rpush.config.batch_size)).to be_empty
    end

    it "does not enqueue a notification that has previously failed delivery" do
      notification
      notification = store.deliverable_notifications(Rpush.config.batch_size).first
      store.mark_failed(notification, 0, "failed", Time.now)
      expect(store.deliverable_notifications(Rpush.config.batch_size)).to be_empty
    end
  end

  describe 'mark_ids_retryable' do
    let(:deliver_after) { time + 10.seconds }

    it 'sets the deliver after timestamp' do
      expect do
        store.mark_ids_retryable([notification.id], deliver_after)
        notification.reload
      end.to change { notification.deliver_after.try(:utc).to_s }.to(deliver_after.utc.to_s)
    end

    it 'ignores IDs that do not exist without throwing an exception' do
      notification.destroy
      expect(logger).to receive(:warn).with("Couldn't find Rpush::Client::Redis::Notification with id=#{notification.id}")
      expect do
        store.mark_ids_retryable([notification.id], deliver_after)
      end.not_to raise_exception
    end
  end

  describe 'mark_ids_failed' do
    it 'marks the notification as failed' do
      expect do
        store.mark_ids_failed([notification.id], nil, '', Time.now)
        notification.reload
      end.to change(notification, :failed).to(true)
    end

    it 'ignores IDs that do not exist without throwing an exception' do
      notification.destroy
      expect(logger).to receive(:warn).with("Couldn't find Rpush::Client::Redis::Notification with id=#{notification.id}")
      expect do
        store.mark_ids_failed([notification.id], nil, '', Time.now)
      end.not_to raise_exception
    end
  end

  describe 'retryable and pending budget' do
    let(:pending_ns)   { Rpush::Client::Redis::Notification.absolute_pending_namespace }
    let(:retryable_ns) { Rpush::Client::Redis::Notification.absolute_retryable_namespace }

    def new_notification
      Rpush::Client::Redis::Apns::Notification.create!(device_token: 'a' * 64, app: app)
    end

    # Creation registers into pending, so a retryable fixture has to be moved across.
    def move_to_retryable(notification, due_at)
      Modis.with_connection do |redis|
        redis.zrem(pending_ns, notification.id)
        redis.zadd(retryable_ns, due_at.to_i, notification.id)
      end
    end

    # The discriminating example: 11 due retries against a budget of 10. Under the
    # previous unbounded claim, retryable_notification_ids returned all 11, `limit`
    # went negative and pending received ZERO -- so the pending notification was
    # absent and the batch overshot the budget. Both assertions fail on that code.
    it 'does not let a deep retryable set starve pending' do
      11.times { move_to_retryable(new_notification, time - 1.hour) }
      pending = new_notification

      delivered = store.deliverable_notifications(10)

      expect(delivered.size).to be <= 10
      expect(delivered).to include(pending)
    end

    it 'gives retryable the whole budget when pending is empty' do
      11.times { move_to_retryable(new_notification, time - 1.hour) }
      Modis.with_connection { |redis| redis.del(pending_ns) }

      expect(store.deliverable_notifications(10).size).to eq 10
    end

    it 'does not claim a retry whose deliver_after is still in the future' do
      move_to_retryable(new_notification, time + 1.hour)
      Modis.with_connection { |redis| redis.del(pending_ns) }

      expect(store.deliverable_notifications(10)).to be_empty
    end

    it 'leaves unclaimed due retries in the retryable set' do
      11.times { move_to_retryable(new_notification, time - 1.hour) }
      Modis.with_connection { |redis| redis.del(pending_ns) }

      store.deliverable_notifications(10)

      remaining = Modis.with_connection { |redis| redis.zcard(retryable_ns) }
      expect(remaining).to eq 1
    end
  end
end if redis?
