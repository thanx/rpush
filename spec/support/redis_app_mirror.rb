# Production runs a hybrid store: apps live in Redis — where Notification#app resolves them and
# the notification store is keyed — AND in ActiveRecord, where the daemon reads them
# (Store::Redis#all_apps / #app use Rpush::Client::ActiveRecord::App; see the "Use ActiveRecord
# to fetch Apps instead of Redis" change). The upstream specs only create Redis apps, so under
# the redis client the daemon can't find them. Mirror every Redis app into ActiveRecord under
# the same id so both sides agree, exactly as production keeps them in sync. Test-only.
module RedisAppMirror
  module_function

  def active_record_class_for(redis_app)
    redis_app.class.name.sub('Rpush::Client::Redis', 'Rpush::Client::ActiveRecord').constantize
  end

  def mirror(redis_app)
    ar_class = active_record_class_for(redis_app)
    record = ar_class.find_or_initialize_by(id: redis_app.id)
    copyable = redis_app.attributes.slice(*ar_class.column_names).except('id', 'type')
    record.assign_attributes(copyable)
    record.id = redis_app.id
    record.save!(validate: false)
  end

  def remove(id)
    Rpush::Client::ActiveRecord::App.where(id: id).delete_all
  end
end

Rpush::Client::Redis::App.class_eval do
  after_save { RedisAppMirror.mirror(self) }
  after_destroy { RedisAppMirror.remove(id) }
end

# The redis Feedback model disables Modis' all-index in production to avoid a huge "all" set.
# Re-enable it for the test suite so specs can list feedback via .all; the data volumes that
# motivate disabling it in production do not exist in tests.
Rpush::Client::Redis::Apns::Feedback.enable_all_index(true)
