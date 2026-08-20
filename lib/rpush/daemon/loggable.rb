module Rpush
  module Daemon
    module Loggable
      def log_debug(msg)
        Rpush.logger.debug(app_prefix(msg))
      end

      def log_info(msg)
        Rpush.logger.info(app_prefix(msg))
      end

      def log_warn(msg)
        Rpush.logger.warn(app_prefix(msg))
      end

      def log_error(e)
        if e.is_a?(Exception)
          Rpush.logger.error(e)
        else
          Rpush.logger.error(app_prefix(e))
        end
      end

      # Emit a single structured push-pipeline log line in logfmt-style
      # `key=value` pairs, so Datadog indexes each field and the lines join to
      # the nexus push logs on `rpush_notification_id`. Field order is stable:
      # event, notification id, app, then the caller's fields in the order given.
      # nil-valued fields are dropped; values with whitespace/`=`/`"` are quoted.
      def log_push_event(event, app: nil, notification: nil, level: :info, **fields)
        parts = ["event=#{event}"]
        parts << "rpush_notification_id=#{notification.id}" if notification
        name = (app || instance_variable_get('@app'))&.name
        parts << "app=#{push_event_value(name)}" unless name.nil?
        fields.each do |key, value|
          next if value.nil?
          parts << "#{key}=#{push_event_value(value)}"
        end
        Rpush.logger.public_send(level, parts.join(' '))
      end

      private

      def push_event_value(value)
        str = value.to_s
        return str unless str.match?(/[\s"=]/)

        %("#{str.gsub('"', '\"')}")
      end

      def app_prefix(msg)
        app = instance_variable_get('@app')
        msg = "[#{app.name}] #{msg}" if app
        msg
      end
    end
  end
end
