module Rpush
  module Client
    module Redis
      class Notification
        include Rpush::MultiJsonHelper
        include Modis::Model
        include Rpush::Client::ActiveModel::Notification

        enable_all_index false # prevent creation of massive rpush:notifications:all set

        before_create :stamp_enqueued_at
        after_create :register_notification

        self.namespace = 'notifications'

        # Delivery priority band for the pending sorted set. Lower band == served first.
        #
        # The pending set is scored so that ascending rank == ascending priority band,
        # then ascending id within a band. The daemon claims by RANK
        # (Store::Redis#pending_notification_ids uses ZRANGE 0..limit +
        # ZREMRANGEBYRANK 0..limit), so ordering the score is sufficient to order
        # delivery -- no change to the claim path is required.
        #
        # BAND is anchored on the DEFAULT class, not on zero, so that a notification
        # at PRIORITY_CLASS_DEFAULT scores exactly its own id. That keeps the pending
        # set byte-identical to pre-priority behaviour for every existing caller, which
        # means (a) no transition window while a mixed-version fleet rolls, (b) no
        # score inversion between old and new writers, and (c) a revert is score-safe.
        # A class below the default scores negative and therefore sorts ahead of all
        # default traffic.
        #
        # BAND must exceed the maximum notification id and stay inside the exact
        # integer range of an IEEE-754 double (2**53), since Redis sorted-set scores
        # are doubles. At 10**12 that holds for ids up to ~9.0e15.
        PRIORITY_CLASS_DEFAULT = 1
        PRIORITY_CLASS_BAND    = 10**12

        def self.absolute_pending_namespace
          "#{absolute_namespace}:pending"
        end

        def self.absolute_retryable_namespace
          "#{absolute_namespace}:retryable"
        end

        attribute :badge, :integer
        attribute :device_token, :string
        attribute :sound, [:string, :hash], strict: false, default: 'default'
        attribute :alert, [:string, :hash], strict: false
        attribute :data, :hash
        attribute :expiry, :integer, default: 1.day.to_i
        attribute :delivered, :boolean
        attribute :delivered_at, :timestamp
        attribute :failed, :boolean
        attribute :failed_at, :timestamp
        attribute :fail_after, :timestamp
        attribute :retries, :integer, default: 0
        attribute :error_code, :integer
        attribute :error_description, :string
        attribute :deliver_after, :timestamp
        attribute :alert_is_json, :boolean
        attribute :sound_is_json, :boolean
        attribute :app_id, :integer
        attribute :collapse_key, :string
        attribute :delay_while_idle, :boolean
        attribute :registration_ids, :array
        attribute :uri, :string
        attribute :priority, :integer
        attribute :url_args, :array
        attribute :category, :string
        attribute :content_available, :boolean, default: false
        attribute :dry_run, :boolean, default: false
        attribute :mutable_content, :boolean, default: false
        attribute :notification, :hash
        attribute :thread_id, :string

        # Delivery band; see PRIORITY_CLASS_BAND. Defaults to PRIORITY_CLASS_DEFAULT so
        # that callers which never set it are indistinguishable from today.
        attribute :priority_class, :integer, default: PRIORITY_CLASS_DEFAULT

        # When the notification entered the pending set. Stamped by a before_create
        # callback rather than an attribute default, because Modis evaluates `default:`
        # once at class-definition time -- a `default: Time.now` would brand every
        # notification with the timestamp of process boot.
        #
        # Exists so the daemon can report how long a notification waited between being
        # enqueued and being handed to APNs/FCM. Nothing else in the model records
        # creation time: there is no created_at attribute.
        attribute :enqueued_at, :timestamp

        def app
          return nil unless app_id
          @app ||= Rpush::Client::Redis::App.find(app_id)
        end

        # Milliseconds between entering the pending set and now, or nil when the
        # notification predates the enqueued_at attribute (every notification already
        # resident in Redis at deploy time). Callers MUST handle nil.
        #
        # enqueued_at has whole-second resolution and is floored, so this is biased
        # 0..999 ms high. That is immaterial against the multi-second budgets it exists
        # to measure, but do not build sub-second assertions on it.
        def age_ms(now = Time.now)
          return nil unless enqueued_at

          ((now - enqueued_at) * 1000).round
        end

        def app=(app)
          @app = app
          if app
            self.app_id = app.id
          else
            self.app_id = nil
          end
        end

        private

        def stamp_enqueued_at
          self.enqueued_at ||= Time.now
        end

        def register_notification
          Modis.with_connection do |redis|
            redis.zadd(self.class.absolute_pending_namespace, pending_score, id)
          end
        end

        # Anchored on the default band so default traffic scores exactly `id`.
        #
        # `|| PRIORITY_CLASS_DEFAULT` rather than `.to_i`: nil.to_i is 0, which would
        # silently PROMOTE a notification whose priority_class failed to load into the
        # highest band -- the unsafe direction. Falling back to the default is inert.
        def pending_score
          band = (priority_class || PRIORITY_CLASS_DEFAULT) - PRIORITY_CLASS_DEFAULT
          (band * PRIORITY_CLASS_BAND) + id
        end
      end
    end
  end
end
