module Rpush
  module Daemon
    module Store
      class Redis
        DEFAULT_MARK_OPTIONS = { persist: true }

        # Ceiling on the share of a poll the retryable set may take in its first pass, so
        # a deep retryable set cannot consume the budget and starve pending. It is a CAP,
        # not a reservation: pending takes everything retryable leaves, and a second
        # retryable pass mops up anything pending did not use -- so neither side starves
        # and no slot goes idle while either side has work.
        RETRYABLE_CLAIM_SHARE = 0.2

        # Claim at most ARGV[2] members scoring at or below ARGV[1] (i.e. DUE), lowest
        # score first, and remove exactly those members.
        #
        # A read-then-remove pair cannot be made safe here even inside MULTI, because
        # MULTI cannot branch on the result of its own ZRANGE: a rank-bounded removal
        # races another daemon's claim and can then remove -- and deliver -- a retry whose
        # backoff has not elapsed. ZREMRANGEBYSCORE is no escape either; it would remove
        # every due member while returning only `limit` of them, losing the rest. EVAL is
        # what makes selection and removal one operation over one member set.
        #
        # `unpack` (Lua 5.1, which is what Redis embeds) is safe against a stack blowout
        # here because the list is bounded by Rpush.config.batch_size.
        RETRYABLE_CLAIM_SCRIPT = <<-LUA
          local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
          if #ids > 0 then
            redis.call('ZREM', KEYS[1], unpack(ids))
          end
          return ids
        LUA

        def app(app_id)
          Rpush::Client::ActiveRecord::App.find(app_id)
        end

        def all_apps
          Rpush::Client::ActiveRecord::App.all
        end

        # Splits a poll's budget between the two sources instead of letting retryable
        # consume all of it.
        #
        # Previously `retryable_notification_ids` claimed EVERY due retry with no
        # bound, then `limit` was decremented by that count. Whenever the due retryable
        # set was at least `limit` deep, pending received exactly zero for the poll --
        # and because the claimed batch also overshot `limit`, the Feeder's
        # `batch_size - AppRunner.total_queued` short-circuit then suppressed subsequent
        # polls until the oversized batch drained, extending the starvation well beyond
        # a single poll. Measured live: a retryable set 35x batch_size deep.
        #
        # Retryable is claimed first but CAPPED, so it can no longer take the poll;
        # pending then claims the ENTIRE remainder, so an empty retryable set costs
        # nothing. Any budget neither side used goes back to retryable, so retryable is
        # not starved when pending is shallow either.
        #
        # The cap sits on retryable rather than on pending on purpose. Reserving a fixed
        # share for RETRYABLE instead -- and giving pending only the rest -- leaves that
        # share unused on every poll where nothing is due, which is every poll of a large
        # campaign drain. That is a flat ~20% off the top of the throughput the feeder is
        # capable of, and the feeder's claim rate is the binding constraint on how long a
        # campaign takes: measured at 5 tasks, deliveries plateaued at 2,672/min/task
        # against the 3,000/min that batch_size 100 over a 2s push_poll allows, i.e. 89%
        # of the claim ceiling, with dispatch, the database and Redis all idle-ish behind
        # it. Losing a fifth of that is 10 minutes on a 2M-notification campaign.
        def deliverable_notifications(limit)
          return [] unless limit > 0

          retryable_ids = retryable_notification_ids(retryable_budget(limit))
          pending_ids   = pending_notification_ids(limit - retryable_ids.size)

          unclaimed = limit - retryable_ids.size - pending_ids.size
          retryable_ids += retryable_notification_ids(unclaimed) if unclaimed > 0

          ids = retryable_ids + pending_ids
          ids.map { |id| find_notification_by_id(id) }.compact
        end

        def mark_delivered(notification, time, opts = {})
          opts = DEFAULT_MARK_OPTIONS.dup.merge(opts)
          notification.delivered = true
          notification.delivered_at = time
          notification.save!(validate: false) if opts[:persist]
        end

        def mark_batch_delivered(notifications)
          now = Time.now
          notifications.each { |n| mark_delivered(n, now) }
        end

        def mark_failed(notification, code, description, time, opts = {})
          opts = DEFAULT_MARK_OPTIONS.dup.merge(opts)
          notification.delivered = false
          notification.delivered_at = nil
          notification.failed = true
          notification.failed_at = time
          notification.error_code = code
          notification.error_description = description
          notification.save!(validate: false) if opts[:persist]
        end

        def mark_batch_failed(notifications, code, description)
          now = Time.now
          notifications.each { |n| mark_failed(n, code, description, now) }
        end

        def mark_ids_failed(ids, code, description, time)
          ids.each do |id|
            notification = find_notification_by_id(id)
            next unless notification

            mark_failed(notification, code, description, time)
          end
        end

        def mark_retryable(notification, deliver_after, opts = {})
          opts = DEFAULT_MARK_OPTIONS.dup.merge(opts)
          notification.delivered = false
          notification.delivered_at = nil
          notification.failed = false
          notification.failed_at = nil
          notification.retries += 1
          notification.deliver_after = deliver_after

          return unless opts[:persist]

          notification.save!(validate: false)
          namespace = Rpush::Client::Redis::Notification.absolute_retryable_namespace
          Modis.with_connection do |redis|
            redis.zadd(namespace, deliver_after.to_i, notification.id)
          end
        end

        def mark_batch_retryable(notifications, deliver_after)
          notifications.each { |n| mark_retryable(n, deliver_after) }
        end

        def mark_ids_retryable(ids, deliver_after)
          ids.each do |id|
            notification = find_notification_by_id(id)
            next unless notification

            mark_retryable(notification, deliver_after)
          end
        end

        def create_apns_feedback(failed_at, device_token, app)
          Rpush::Client::Redis::Apns::Feedback.create!(failed_at: failed_at, device_token: device_token, app_id: app.id)
        end

        def create_fcm_notification(attrs, data, app)
          notification = Rpush::Client::Redis::Fcm::Notification.new
          create_fcm_like_notification(notification, attrs, data, app)
        end

        def create_gcm_notification(attrs, data, registration_ids, deliver_after, app)
          notification = Rpush::Client::Redis::Gcm::Notification.new
          create_gcm_like_notification(notification, attrs, data, registration_ids, deliver_after, app)
        end

        def create_adm_notification(attrs, data, registration_ids, deliver_after, app)
          notification = Rpush::Client::Redis::Adm::Notification.new
          create_gcm_like_notification(notification, attrs, data, registration_ids, deliver_after, app)
        end

        def update_app(app)
          app.save!
        end

        def update_notification(notification)
          notification.save!
        end

        def release_connection
        end

        def reopen_log
        end

        def pending_delivery_count
          Modis.with_connection do |redis|
            pending = redis.zrange(Rpush::Client::Redis::Notification.absolute_pending_namespace, 0, -1)
            retryable = redis.zrangebyscore(Rpush::Client::Redis::Notification.absolute_retryable_namespace, 0, Time.now.to_i)

            pending.count + retryable.count
          end
        end

        def translate_integer_notification_id(id)
          id
        end

        private

        def find_notification_by_id(id)
          Rpush::Client::Redis::Notification.find(id)
        rescue Modis::RecordNotFound
          Rpush.logger.warn("Couldn't find Rpush::Client::Redis::Notification with id=#{id}")
          nil
        end

        def create_fcm_like_notification(notification, attrs, data, app) # rubocop:disable Metrics/ParameterLists
          notification.assign_attributes(attrs)
          notification.data = data
          notification.app = app
          notification.save!
          notification
        end

        def create_gcm_like_notification(notification, attrs, data, registration_ids, deliver_after, app) # rubocop:disable Metrics/ParameterLists
          notification.assign_attributes(attrs)
          notification.data = data
          notification.registration_ids = registration_ids
          notification.deliver_after = deliver_after
          notification.app = app
          notification.save!
          notification
        end

        # Claims at most `limit` DUE retries, oldest-due first.
        #
        # The retryable set is scored by `deliver_after.to_i` (see #mark_retryable), so
        # ascending rank is ascending due-time. Because `claim` is capped at the number
        # of members scoring <= now, the lowest `claim` members by rank are all due --
        # which is what makes a rank-bounded claim safe here. ZRANGE and
        # ZREMRANGEBYRANK inside the MULTI address an identical member set, so nothing
        # is ever removed without being returned.
        #
        # Concurrency note: with several daemon processes, another may claim between the
        # ZCOUNT and the MULTI. `claim` can then exceed the remaining due count and the
        # removal may take a member that is not yet due, delivering one retry earlier
        # than its backoff intended. That is strictly less harmful than the previous
        # all-or-nothing race over the entire due set, and it cannot lose a
        # notification.
        # Retryable's first-pass cap. `.min` with `limit` matters at limit == 1, which the
        # Feeder reaches whenever AppRunner still holds batch_size - 1 queued: without it
        # the ceil would ask for more than the poll has.
        #
        # Nothing is reserved for pending here, because nothing needs to be: pending
        # claims `limit - retryable_ids.size`, so it always receives at least
        # limit - ceil(limit * SHARE), and the whole budget whenever nothing is due.
        def retryable_budget(limit)
          [(limit * RETRYABLE_CLAIM_SHARE).ceil, limit].min
        end

        def retryable_notification_ids(limit)
          return [] unless limit > 0

          Modis.with_connection do |redis|
            redis.eval(
              RETRYABLE_CLAIM_SCRIPT,
              keys: [Rpush::Client::Redis::Notification.absolute_retryable_namespace],
              argv: [Time.now.to_i, limit]
            )
          end
        end

        # NOTE the early return. ZRANGE bounds are INCLUSIVE, so the `limit - 1` below
        # turns a request for 0 into `zrange 0 0`, which returns ONE id -- claiming a
        # notification the caller had no budget for and, worse, removing it from the
        # pending set to do so. The previous caller papered over that with its own
        # `limit > 0 ?` ternary; guarding here fixes it for every caller instead.
        def pending_notification_ids(limit)
          return [] unless limit > 0

          limit = [0, limit - 1].max # 'zrange key 0 1' will return 2 values, not 1.
          pending_ns = Rpush::Client::Redis::Notification.absolute_pending_namespace

          Modis.with_connection do |redis|
            pending_results = redis.multi do |transaction|
              transaction.zrange(pending_ns, 0, limit)
              transaction.zremrangebyrank(pending_ns, 0, limit)
            end

            pending_results.first
          end
        end
      end
    end
  end
end

Rpush::Daemon::Store::Interface.check(Rpush::Daemon::Store::Redis)
