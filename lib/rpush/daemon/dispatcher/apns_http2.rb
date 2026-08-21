module Rpush
  module Daemon
    module Dispatcher
      class ApnsHttp2
        include Loggable
        include Reflectable

        URLS = {
          production: 'https://api.push.apple.com:443',
          development: 'https://api.sandbox.push.apple.com:443',
          sandbox: 'https://api.sandbox.push.apple.com:443'
        }

        DEFAULT_TIMEOUT = 60

        def initialize(app, delivery_class, _options = {})
          @app = app
          @delivery_class = delivery_class

          @client = create_http2_client(app)
        end

        def dispatch(payload)
          @delivery_class.new(@app, @client, payload.batch).perform
        end

        def cleanup
          @client.close
        end

        private

        def create_http2_client(app)
          url = URLS[app.environment.to_sym]
          client = NetHttp2::Client.new(url, ssl_context: prepare_ssl_context, connect_timeout: DEFAULT_TIMEOUT)
          client.on(:error) do |error|
            # Include the message, not just the class: this is the one line meant to make a
            # mid-flight reset findable by app and time in Datadog, and two different socket
            # errors of the same class (e.g. two distinct SSLError causes) are otherwise
            # indistinguishable here.
            log_push_event(:connection_error, level: :error, error: "#{error.class}: #{error.message}")
            reflect(:error, error)
          end
          client
        end

        def prepare_ssl_context
          @ssl_context ||= begin
            ctx = OpenSSL::SSL::SSLContext.new
            begin
              p12      = OpenSSL::PKCS12.new(@app.certificate, @app.password)
              ctx.key  = p12.key
              ctx.cert = p12.certificate
            rescue OpenSSL::PKCS12::PKCS12Error
              ctx.key  = OpenSSL::PKey::RSA.new(@app.certificate, @app.password)
              ctx.cert = OpenSSL::X509::Certificate.new(@app.certificate)
            end
            ctx
          end
        end
      end
    end
  end
end
