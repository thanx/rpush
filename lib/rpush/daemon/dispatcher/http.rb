module Rpush
  module Daemon
    module Dispatcher
      class Http
        def initialize(app, delivery_class, _options = {})
          @app = app
          @delivery_class = delivery_class
          @http = Net::HTTP::Persistent.new(name: 'rpush')
        end

        # A refused notification never reaches its delivery class, so no request is built
        # or sent; it still counts as processed so the batch completes and persists it.
        def dispatch(payload)
          return payload.batch.notification_processed unless Admission.admit(payload.notification, payload.batch)

          @delivery_class.new(@app, @http, payload.notification, payload.batch).perform
        end

        def cleanup
          @http.shutdown
        end
      end
    end
  end
end
