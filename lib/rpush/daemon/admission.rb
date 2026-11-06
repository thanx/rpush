module Rpush
  module Daemon
    # Pre-send admission: the one place a host application can refuse a notification before
    # any provider IO. Reflections cannot do this: they run after the batch completes and
    # swallow their errors.
    #
    # Rpush.config.admission is an object responding to #call(notification). Only a return
    # value of exactly `true` admits. Any other value refuses, and a Symbol or String names
    # the reason. An exception also refuses (reason `admission_error`): a hook that cannot
    # decide must never let the notification reach the network. A refused notification is
    # marked failed with REFUSED_DESCRIPTION and is never retried. With no hook configured
    # every notification is admitted and nothing else changes.
    #
    # Transports call this per notification, immediately before building that notification's
    # request, on the dispatcher thread that will submit it.
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
