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

    # Retryable's cap is a first-pass cap only: a second pass hands back whatever pending
    # left, so a retry backlog still drains at the full batch_size when pending is empty.
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

    # THE example for the campaign target: a drain with nothing due must get the WHOLE
    # batch, not batch minus retryable's share. Reserving that share instead of capping it
    # idles a fifth of the feeder's claim rate on every poll of a campaign -- and the
    # claim rate is what sets how long the campaign takes.
    it 'gives pending the whole budget when nothing is due to retry' do
      10.times { new_notification }

      expect(store.deliverable_notifications(10).size).to eq 10
    end

    # limit == 1 is what the Feeder passes whenever AppRunner still holds batch_size - 1
    # queued.
    it 'delivers pending work when the whole budget is a single slot and nothing is due' do
      pending = new_notification

      expect(store.deliverable_notifications(1)).to eq [pending]
    end

    # ZRANGE bounds are INCLUSIVE, so a zero pending budget used to claim one id anyway
    # (`zrange 0 0` returns one element). Both halves are asserted, because the harmful
    # half is the second: the notification was REMOVED from the pending set to be
    # returned over budget, so a caller that dropped the overflow would drop the
    # notification with it.
    it 'gives the single slot to the due retry when there is one' do
      retryable = new_notification
      move_to_retryable(retryable, time - 1.hour)
      new_notification

      expect(store.deliverable_notifications(1)).to eq [retryable]
    end

    it 'leaves the pending notification in the set when its budget was zero' do
      retryable = new_notification
      move_to_retryable(retryable, time - 1.hour)
      new_notification

      store.deliverable_notifications(1)

      remaining = Modis.with_connection { |redis| redis.zcard(pending_ns) }
      expect(remaining).to eq 1
    end

    it 'delivers nothing rather than raising when the budget is zero' do
      new_notification

      expect(store.deliverable_notifications(0)).to be_empty
    end

    # The claim selects and removes ONE member set in a single Redis operation, so a
    # concurrent claimer cannot make it remove a member whose backoff has not elapsed.
    # Under a rank-bounded claim the future retry would be taken here as soon as the
    # due count read before the removal disagreed with the set.
    it 'leaves a future retry queued when it shares the set with a due one' do
      due = new_notification
      future = new_notification
      move_to_retryable(due, time - 1.hour)
      move_to_retryable(future, time + 1.hour)
      Modis.with_connection { |redis| redis.del(pending_ns) }

      expect(store.deliverable_notifications(10)).to eq [due]
    end

    it 'keeps the future retry in the retryable set for its own backoff' do
      due = new_notification
      future = new_notification
      move_to_retryable(due, time - 1.hour)
      move_to_retryable(future, time + 1.hour)

      store.deliverable_notifications(10)

      remaining = Modis.with_connection { |redis| redis.zrange(retryable_ns, 0, -1) }
      expect(remaining).to eq [future.id.to_s]
    end
  end
end if redis?
