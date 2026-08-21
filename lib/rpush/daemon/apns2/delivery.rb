module Rpush
  module Daemon
    module Apns2
      # https://developer.apple.com/library/content/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/CommunicatingwithAPNs.html

      HTTP2_HEADERS_KEY = 'headers'

      class Delivery < Rpush::Daemon::Delivery
        RETRYABLE_CODES = [ 429, 500, 503 ]
        CLIENT_JOIN_TIMEOUT = 60
        # How long to defer a notification whose delivery could not be confirmed at the
        # transport level: a dropped connection, an SSL failure, or a stream that closed
        # with no APNs status. Matches the service-unavailable backoff.
        RECONNECT_RETRY_DELAY = 10.seconds

        def initialize(app, http2_client, batch)
          @app = app
          @client = http2_client
          @batch = batch
        end

        def perform
          @batch.each_notification do |notification|
            begin
              prepare_async_post(notification)
            rescue OpenSSL::SSL::SSLError
              # The TLS handshake/write failed before this notification got a stream, so it
              # holds no outcome. Re-queue it (and keep processing the rest of the batch)
              # instead of dropping it with only a log line.
              connection_lost(notification)
            end
          end

          # Send all preprocessed requests at once
          @client.join(timeout: CLIENT_JOIN_TIMEOUT)

          # A dropped connection tears down its in-flight streams WITHOUT raising here:
          # net-http2 hands the socket error to the client's on(:error) callback and #join
          # returns once the stream set is emptied. Those notifications never received an
          # on(:close), so they hold no outcome and would be silently discarded when the
          # batch completes. Re-queue them instead. No-op on the normal path.
          retry_unresolved
        rescue NetHttp2::AsyncRequestTimeout => error
          mark_batch_retryable(Time.now + RECONNECT_RETRY_DELAY, error)
          @client.close
          raise
        rescue Errno::ECONNREFUSED, SocketError, Errno::ECONNRESET => error
          mark_batch_retryable(Time.now + RECONNECT_RETRY_DELAY, error)
          raise
        rescue StandardError => error
          mark_batch_failed(error)
          raise
        ensure
          @batch.all_processed
        end

        protected
        ######################################################################

        def prepare_async_post(notification)
          response = {}

          request = build_request(notification)
          http_request = @client.prepare_request(:post, request[:path],
            body:    request[:body],
            headers: request[:headers]
          )

          http_request.on(:headers) do |hdrs|
            response[:code] = hdrs[':status'].to_i
          end

          http_request.on(:body_chunk) do |body_chunk|
            next unless body_chunk.present?

            response[:failure_reason] = JSON.parse(body_chunk)['reason']
          end

          http_request.on(:close) { handle_response(notification, response) }

          @client.call_async(http_request)
        end

        def handle_response(notification, response)
          code = response[:code]
          case code
          when 200
            ok(notification)
          when *RETRYABLE_CODES
            service_unavailable(notification, response)
          when nil
            # The stream closed before any :status header arrived — APNs returned no verdict
            # (the connection dropped mid-flight). Transport failure, not an APNs rejection,
            # so retry rather than mark it permanently failed.
            connection_lost(notification)
          else
            reflect(:notification_id_failed,
              @app,
              notification.id, code,
              response[:failure_reason])
            @batch.mark_failed(notification, response[:code], response[:failure_reason])
            failed_message_to_log(notification, response)
          end
        end

        def ok(notification)
          log_push_event(:delivered, notification: notification,
            device_token: truncate_device_token(notification.device_token))
          @batch.mark_delivered(notification)
        end

        def service_unavailable(notification, response)
          @batch.mark_retryable(notification, Time.now + RECONNECT_RETRY_DELAY)
          # A retryable APNs response (429/500/503) is not a failure: log it only as a retry
          # so the failure signal stays clean. Logs go last, after mark_retryable has set the
          # deliver_after we want to display.
          retry_message_to_log(notification, reason: response[:code])
        end

        # Re-queue every notification the batch never resolved — one whose HTTP/2 stream was
        # abandoned when the connection dropped, so its on(:close) never fired and it holds no
        # delivered/failed/retryable outcome. A no-op when every stream reported a result.
        def retry_unresolved
          @batch.unresolved.each { |notification| connection_lost(notification) }
        end

        # A notification with no delivery outcome from APNs: retry it on a fresh connection
        # rather than discard it. Shared by the mid-flight-drop sweep (#retry_unresolved), the
        # no-status branch of #handle_response, and an SSL failure during #perform.
        def connection_lost(notification)
          @batch.mark_retryable(notification, Time.now + RECONNECT_RETRY_DELAY)
          retry_message_to_log(notification, reason: 'connection_lost')
        end

        def build_request(notification)
          {
            path:    "/3/device/#{notification.device_token}",
            headers: prepare_headers(notification),
            body:    prepare_body(notification)
          }
        end

        def prepare_body(notification)
          hash = notification.as_json.except(HTTP2_HEADERS_KEY)
          JSON.dump(hash).force_encoding(Encoding::BINARY)
        end

        def prepare_headers(notification)
          headers = {}

          headers['apns-expiration'] = '0'
          headers['apns-priority'] = '10'
          headers['apns-topic'] = @app.bundle_id
          headers['apns-push-type'] = 'background' if notification.content_available?

          headers.merge notification_data(notification)[HTTP2_HEADERS_KEY] || {}
        end

        def notification_data(notification)
          notification.data || {}
        end

        def retry_message_to_log(notification, reason:)
          log_push_event(:retrying, notification: notification, level: :warn,
            reason: reason,
            retry: notification.retries,
            deliver_after: notification.deliver_after&.strftime('%Y-%m-%d %H:%M:%S'))
        end

        def failed_message_to_log(notification, response)
          log_push_event(:failed, notification: notification, level: :error,
            code: response[:code], reason: response[:failure_reason])
        end

        # Keep the raw device token out of logs; a short prefix is enough to correlate.
        def truncate_device_token(token)
          return token if token.nil? || token.length <= 8

          "#{token[0, 8]}…"
        end
      end
    end
  end
end
