module Rpush
  module Daemon
    module Apns
      class Delivery < Rpush::Daemon::Delivery
        def initialize(app, connection, batch)
          @app = app
          @connection = connection
          @batch = batch
        end

        def perform
          binary = batch_to_binary
          @connection.write(binary) unless binary.empty?
          mark_batch_delivered
          describe_deliveries
        rescue Rpush::Daemon::TcpConnectionError => error
          mark_batch_retryable(Time.now + 10.seconds, error)
          raise
        rescue StandardError => error
          mark_batch_failed(error)
          raise
        ensure
          @batch.all_processed
        end

        protected

        def batch_to_binary
          payload = ""
          @admitted = []
          @batch.each_notification do |notification|
            next unless admitted?(notification)

            @admitted << notification
            payload << notification.to_binary
          end
          payload
        end

        def describe_deliveries
          @admitted.each do |notification|
            log_info("#{notification.id} sent to #{notification.device_token}")
          end
        end
      end
    end
  end
end
