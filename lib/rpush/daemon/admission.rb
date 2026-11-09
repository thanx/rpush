module Rpush
  module Daemon
    # The host hook runs before provider IO. Only literal true permits a send.
    # A hook error or any other result records a terminal refusal.
    module Admission
      extend Reflectable
      extend Loggable

      REFUSED_DESCRIPTION = 'Refused before send'.freeze
      ERROR_REASON = 'admission_error'.freeze
      DEFAULT_REASON = 'refused'.freeze

      def self.admit(notification, batch)
        hook = Rpush.config.admission
        return true if hook.nil?

        decision = decide(hook, notification)
        return true if true.equal?(decision)

        reason = decision.is_a?(Symbol) || decision.is_a?(String) ? decision.to_s : DEFAULT_REASON
        batch.mark_refused(notification, "#{REFUSED_DESCRIPTION}: #{reason}")
        log_push_event(:refused, notification: notification, level: :warn, reason: reason)
        reflect(:notification_refused, notification, reason)
        false
      end

      def self.decide(hook, notification)
        hook.call(notification)
      rescue StandardError => e
        log_push_event(:admission_error, notification: notification, level: :error, error: e.class.name)
        reflect(:error, e)
        ERROR_REASON
      end
      private_class_method :decide
    end
  end
end
