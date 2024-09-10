module Rpush
  class ReflectionCollection
    class NoSuchReflectionError < StandardError; end

    REFLECTIONS = [
      :apns_feedback, :notification_enqueued, :notification_delivered,
      :notification_failed, :notification_will_retry,
      :gcm_delivered_to_recipient, :gcm_failed_to_recipient, :gcm_canonical_id, :gcm_invalid_registration_id,
      :fcm_delivered_to_recipient, :fcm_failed_to_recipient, :fcm_canonical_id, :fcm_invalid_device_token,
      :error, :adm_canonical_id, :adm_failed_to_recipient, :wns_invalid_channel,
      :tcp_connection_lost, :ssl_certificate_will_expire, :ssl_certificate_revoked,
      :notification_id_will_retry, :notification_id_failed
    ]

    DEPRECATIONS = {}

    REFLECTIONS.each do |reflection|
      class_eval(<<-RUBY, __FILE__, __LINE__)
        def #{reflection}(*args, &blk)
          Rpush.logger.warn("RPush __dispatch[20] - block_given?: #{block_given?}")
          raise "block required" unless block_given?
          @reflections[:#{reflection}] = blk
        end
      RUBY
    end

    def initialize
      @reflections = {}
    end

    def __dispatch(reflection, *args)
      Rpush.logger.warn("RPush __dispatch[32] - reflection: #{reflection}")
      blk = @reflections[reflection]

      Rpush.logger.warn("RPush __dispatch[35] - blk: #{blk}")

      if blk
        blk.call(*args)

        if DEPRECATIONS.key?(reflection)
          Rpush.logger.warn("RPush __dispatch[41] - DEPRECATIONS.key?(reflection): #{DEPRECATIONS.key?(reflection)}")
          replacement, removal_version = DEPRECATIONS[reflection]
          Rpush::Deprecation.warn("#{reflection} is deprecated and will be removed in version #{removal_version}. Use #{replacement} instead.")
        end
      elsif !REFLECTIONS.include?(reflection)
        Rpush.logger.warn("RPush __dispatch[46] - !REFLECTIONS.include?(reflection): #{!REFLECTIONS.include?(reflection)}")
        raise NoSuchReflectionError, reflection
      end
    end
  end
end
