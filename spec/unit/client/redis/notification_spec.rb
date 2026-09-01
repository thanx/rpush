require 'unit_spec_helper'

describe Rpush::Client::Redis::Notification do
  it_behaves_like 'Rpush::Client::Notification'

  let(:app) do
    Rpush::Client::Redis::Apns::App.create!(name: 'my_app', environment: 'development', certificate: TEST_CERT)
  end

  def create_notification(attrs = {})
    Rpush::Client::Redis::Apns::Notification.create!({ device_token: 'a' * 64, app: app }.merge(attrs))
  end

  def pending_score_for(notification)
    Modis.with_connection do |redis|
      redis.zscore(described_class.absolute_pending_namespace, notification.id)
    end
  end

  def pending_ids_in_rank_order
    Modis.with_connection do |redis|
      redis.zrange(described_class.absolute_pending_namespace, 0, -1).map(&:to_i)
    end
  end

  describe 'pending-set score' do
    it 'scores a default notification as exactly its own id' do
      notification = create_notification
      expect(pending_score_for(notification)).to eq notification.id.to_f
    end

    it 'scores a priority-class-0 notification one band below its id' do
      notification = create_notification(priority_class: 0)
      expected = notification.id - described_class::PRIORITY_CLASS_BAND
      expect(pending_score_for(notification)).to eq expected.to_f
    end

    it 'ranks a priority-class-0 notification ahead of a default one created earlier' do
      earlier_default = create_notification
      later_priority  = create_notification(priority_class: 0)

      ranked = pending_ids_in_rank_order
      expect(ranked.index(later_priority.id)).to be < ranked.index(earlier_default.id)
    end

    it 'treats an explicitly nil priority_class as the default rather than promoting it' do
      notification = create_notification(priority_class: nil)
      expect(pending_score_for(notification)).to eq notification.id.to_f
    end

    it 'keeps the band inside the exact-integer range of a double' do
      expect(described_class::PRIORITY_CLASS_BAND).to be < 2**53
    end
  end

  describe 'priority_class' do
    it 'defaults to PRIORITY_CLASS_DEFAULT' do
      expect(create_notification.priority_class).to eq described_class::PRIORITY_CLASS_DEFAULT
    end
  end

  describe 'enqueued_at' do
    it 'is stamped on create' do
      expect(create_notification.enqueued_at).not_to be_nil
    end

    it 'differs between notifications created at different times' do
      first = nil
      second = nil
      Timecop.freeze(Time.now) { first = create_notification }
      Timecop.freeze(Time.now + 5.minutes) { second = create_notification }

      expect(second.enqueued_at.to_i).to be > first.enqueued_at.to_i
    end

    it 'is not overwritten when the caller supplies one' do
      supplied = Time.now - 1.hour
      expect(create_notification(enqueued_at: supplied).enqueued_at.to_i).to eq supplied.to_i
    end
  end

  describe '#age_ms' do
    it 'is nil when enqueued_at is absent' do
      notification = create_notification
      notification.enqueued_at = nil
      expect(notification.age_ms).to be_nil
    end

    it 'measures whole seconds from enqueued_at' do
      notification = create_notification
      notification.enqueued_at = Time.now - 3
      expect(notification.age_ms(Time.now)).to be_within(999).of(3000)
    end
  end
end if redis?
