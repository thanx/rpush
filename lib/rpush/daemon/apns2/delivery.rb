module Rpush
  module Daemon
    module Apns2
      # https://developer.apple.com/library/content/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/CommunicatingwithAPNs.html

      HTTP2_HEADERS_KEY = 'headers'

      class Delivery < Rpush::Daemon::Delivery
        RETRYABLE_CODES = [ 429, 500, 503 ]
        CLIENT_JOIN_TIMEOUT = 60
        # How long to defer a notification whose delivery could not be confirmed at the
        # transport level: a dropped connection, or a stream that closed with no APNs
        # status. Matches the existing service-unavailable / connection-error backoff.
        # Mirrors Apnsp8::Delivery.
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
            rescue OpenSSL::SSL::SSLError => error
              # Building the request for THIS notification raised before it ever got a
              # stream, so it will never receive an on(:close) — left alone it would hold no
              # outcome at all (neither delivered, failed, nor retryable) and silently
              # vanish from the batch when the next notification is processed. Retry it
              # explicitly instead of just logging and moving on.
              prepare_failed(notification, error)
            end
          end

          # Send all preprocessed requests at once
          @client.join(timeout: CLIENT_JOIN_TIMEOUT)

          # A dropped connection tears down its in-flight streams WITHOUT raising here:
          # net-http2 hands the socket error to the client's on(:error) callback and #join
          # returns once the stream set is emptied. Those notifications never received an
          # on(:close), so they hold no outcome and would be silently discarded when the
          # batch completes. Re-queue them so the frame lands on a fresh connection instead
          # of vanishing. No-op on the normal path where every stream reported a result.
          # (Mirrors Apnsp8::Delivery#perform — see PR #7.)
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
            # The stream closed before any :status header arrived — APNs returned no
            # verdict (the connection dropped mid-flight). This is a transport failure, not
            # an APNs rejection, so retry rather than mark it permanently failed.
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
          # A retryable APNs response (429/500/503) is not a failure: log it only as a
          # retry so the failure signal stays clean. Logs go last, after mark_retryable
          # has set the deliver_after we want to display.
          retry_message_to_log(notification, reason: response[:code])
        end

        # Re-queue every notification the batch never resolved — one whose HTTP/2 stream was
        # abandoned when the connection dropped, so its on(:close) never fired and it holds
        # no delivered/failed/retryable outcome. A no-op when every stream reported a result.
        def retry_unresolved
          @batch.unresolved.each { |notification| connection_lost(notification) }
        end

        # A notification with no delivery outcome from APNs: retry it on a fresh connection
        # rather than discard it. Shared by the mid-flight-drop sweep (#retry_unresolved) and
        # the no-status branch of #handle_response.
        def connection_lost(notification)
          @batch.mark_retryable(notification, Time.now + RECONNECT_RETRY_DELAY)
          retry_message_to_log(notification, reason: 'connection_lost')
        end

        # The per-notification request-preparation step raised before its stream existed
        # (e.g. an SSL renegotiation error on this cert-based connection), so it will never
        # receive an on(:close). Retry it like any other transport-level drop rather than
        # silently skipping to the next notification in the batch.
        def prepare_failed(notification, error)
          @batch.mark_retryable(notification, Time.now + RECONNECT_RETRY_DELAY)
          # Keep `reason` a stable, low-cardinality value for classification and put the
          # exception in its own `error` field, consistent with the dispatcher's
          # connection_error event.
          retry_message_to_log(notification, reason: 'prepare_failed',
            error: "#{error.class}: #{error.message}")
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

          headers.merge notification_data(notification)[HTTP2_HEADERS_KEY] || {}
        end

        def notification_data(notification)
          notification.data || {}
        end

        def retry_message_to_log(notification, reason:, error: nil)
          log_push_event(:retrying, notification: notification, level: :warn,
            reason: reason,
            error: error,
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
