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

        # This dispatch context stays in memory for the outcome reflections.
        # It is not stored and is not sent to a provider.
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
