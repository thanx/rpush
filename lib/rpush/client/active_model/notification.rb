module Rpush
  module Client
    module ActiveModel
      module Notification
        def self.included(base)
          base.instance_eval do
            validates :expiry, numericality: true, allow_nil: true
            validates :app, presence: true
          end
        end

        # In memory only, never persisted or sent: whatever the pre-send admission hook
        # attached for this dispatch (e.g. an attempt token), so the outcome reflections
        # for the same notification object can settle it.
        attr_accessor :admission_context

        def payload
          multi_json_dump(as_json)
        end

        def payload_data_size
          multi_json_dump(as_json['data']).bytesize
        end
      end
    end
  end
end
