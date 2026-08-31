module Rpush
  module Daemon
    module Store
      class Redis
        DEFAULT_MARK_OPTIONS = { persist: true }

        # Fraction of a poll's budget reserved for the retryable set, so that a large
        # pending backlog can never starve retries. The remainder goes to pending, and
        # retryable additionally absorbs whatever pending does not use -- so when
        # pending is empty, retries drain at the full batch_size.
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
        # Pending is claimed FIRST and capped, so it is never starved; retryable takes
        # the remainder plus anything pending left unused, so it is never starved either.
        def deliverable_notifications(limit)
          return [] unless limit > 0

          pending_ids   = pending_notification_ids(pending_budget(limit))
          retryable_ids = retryable_notification_ids(limit - pending_ids.size)

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
        # Pending's share of a poll, floored at one slot.
        #
        # The floor is load-bearing at limit == 1, which the Feeder reaches whenever
        # AppRunner has batch_size - 1 notifications still queued: 1 - (1 * 0.2).ceil is
        # 0, so without the floor pending would be skipped entirely for that poll and a
        # poll with no due retry would return nothing at all while pending work waited.
        # The single slot goes to pending because pending is the latency-bearing side;
        # retryable still gets it through `limit - pending_ids.size` when pending is empty.
        def pending_budget(limit)
          [limit - (limit * RETRYABLE_CLAIM_SHARE).ceil, 1].max
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

        def pending_notification_ids(limit)
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
