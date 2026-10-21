module Rpush
  module Daemon
    class Feeder
      extend Reflectable
      extend Loggable

      def self.start(push_mode = false)
        self.should_stop = false

        @thread = Thread.new do
          push_mode ? feed_all : feed_forever
          Rpush::Daemon.store.release_connection
        end

        @thread.join
      rescue StandardError => e
        log_error(e)
        reflect(:error, e)
      ensure
        @thread = nil
      end

      def self.stop
        self.should_stop = true
        interruptible_sleeper.stop
        @thread.join if @thread
      rescue StandardError => e
        log_error(e)
        reflect(:error, e)
      ensure
        @thread = nil
      end

      def self.wakeup
        interruptible_sleeper.stop
      end

      class << self
        attr_accessor :should_stop
      end

      def self.feed_all
        enqueue_notifications until Rpush::Daemon.store.pending_delivery_count == 0
      end

      def self.feed_forever
        loop do
          enqueue_notifications
          interruptible_sleeper.sleep(Rpush.config.push_poll)
          return if should_stop
        end
      end

      def self.enqueue_notifications
        poll_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        queued_before = Rpush::Daemon::AppRunner.total_queued
        queue_ms = elapsed_ms(poll_started)
        batch_size = Rpush.config.batch_size - queued_before
        if batch_size <= 0
          reflect(:feeder_poll, requested: 0, claimed: 0, queued_before: queued_before,
            queue_ms: queue_ms, claim_ms: 0, enqueue_ms: 0, work_ms: elapsed_ms(poll_started))
          return
        end

        claim_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        notifications = Rpush::Daemon.store.deliverable_notifications(batch_size)
        claim_ms = elapsed_ms(claim_started)

        enqueue_started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Rpush::Daemon::AppRunner.enqueue(notifications)
        reflect(:feeder_poll, requested: batch_size, claimed: notifications.size, queued_before: queued_before,
          queue_ms: queue_ms, claim_ms: claim_ms, enqueue_ms: elapsed_ms(enqueue_started),
          work_ms: elapsed_ms(poll_started))
      rescue StandardError => e
        Rpush.logger.error(e)
        reflect(:error, e)
      end

      def self.interruptible_sleeper
        @interruptible_sleeper ||= InterruptibleSleep.new
      end

      def self.elapsed_ms(started_at)
        ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at) * 1000).round(2)
      end
    end
  end
end
