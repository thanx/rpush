module Rpush
  module Daemon
    class DispatcherLoop
      include Reflectable
      include Loggable

      attr_reader :started_at, :dispatch_count

      STOP = :stop

      def initialize(queue, dispatcher)
        @queue = queue
        @dispatcher = dispatcher
        @dispatch_count = 0
      end

      def thread_status
        @thread ? @thread.status : 'not started'
      end

      def start
        @started_at = Time.now

        @thread = Thread.new do
          loop do
            payload = @queue.pop
            if stop_payload?(payload)
              break if should_stop?(payload)

              # Intended for another dispatcher loop.
              @queue.push(payload)
              Thread.pass
              sleep 0.1
            else
              dispatch(payload)
            end
          end

          Rpush::Daemon.store.release_connection
        end
      end

      def stop
        @queue.push([STOP, object_id]) if @thread
        @thread.join if @thread
        @dispatcher.cleanup
      rescue StandardError => e
        log_error(e)
        reflect(:error, e)
      ensure
        @thread = nil
      end

      private

      def stop_payload?(payload)
        payload.is_a?(Array) && payload.first == STOP
      end

      def should_stop?(payload)
        payload.last == object_id
      end

      def dispatch(payload)
        @dispatch_count += 1
        if simulate_delivery?
          # Keep the feeder, app queue, and Redis persistence in the benchmark, but
          # never enter a provider dispatcher. A batch payload has no notification;
          # single-delivery services share a batch across their queue payloads.
          if payload.notification
            payload.batch.mark_simulated(payload.notification)
            payload.batch.notification_processed
          else
            payload.batch.mark_all_simulated
            payload.batch.all_processed
          end
        else
          @dispatcher.dispatch(payload)
        end
      rescue StandardError => e
        log_error(e)
        reflect(:error, e)
      end

      # This disposable POC branch deliberately blocks *all* provider sends in
      # sandbox, including notifications from other sandbox users. Production and
      # every other environment keep the normal dispatcher path. Do not merge
      # this branch as the long-lived Rpush default.
      def simulate_delivery?
        ENV['RAILS_ENV'] == 'sandbox'
      end
    end
  end
end
