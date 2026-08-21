require 'unit_spec_helper'

describe Rpush::Daemon::Loggable do
  let(:logger) { double(info: nil, warn: nil, error: nil) }
  before { allow(Rpush).to receive_messages(logger: logger) }

  let(:klass) do
    Class.new do
      include Rpush::Daemon::Loggable
      def initialize(app = nil)
        @app = app
      end
    end
  end

  let(:app) { double(name: 'my_app') }
  let(:notification) { double(id: 42) }

  describe '#log_push_event' do
    it 'writes a structured line with event, notification id, app, and fields at info level' do
      obj = klass.new(app)
      expect(logger).to receive(:info).with('event=delivered rpush_notification_id=42 app=my_app code=200')
      obj.log_push_event(:delivered, notification: notification, code: 200)
    end

    it 'routes the warn level to the warn logger' do
      obj = klass.new(app)
      expect(logger).to receive(:warn).with('event=retrying rpush_notification_id=42 app=my_app reason=503')
      obj.log_push_event(:retrying, notification: notification, level: :warn, reason: 503)
    end

    it 'routes the error level to the error logger' do
      obj = klass.new(app)
      expect(logger).to receive(:error).with('event=failed rpush_notification_id=42 app=my_app code=410 reason=Unregistered')
      obj.log_push_event(:failed, notification: notification, level: :error, code: 410, reason: 'Unregistered')
    end

    it 'omits the notification id when no notification is given' do
      obj = klass.new(app)
      expect(logger).to receive(:error).with('event=connection_error app=my_app error=Errno::ECONNRESET')
      obj.log_push_event(:connection_error, level: :error, error: 'Errno::ECONNRESET')
    end

    it 'prefers an explicitly passed app over the instance app' do
      obj = klass.new(nil)
      other = double(name: 'other_app')
      expect(logger).to receive(:info).with('event=startup_failed app=other_app reason="bad cert"')
      obj.log_push_event(:startup_failed, app: other, reason: 'bad cert')
    end

    it 'quotes field values that contain whitespace' do
      obj = klass.new(app)
      expect(logger).to receive(:warn).with('event=retrying rpush_notification_id=42 app=my_app deliver_after="2026-08-20 16:21:36"')
      obj.log_push_event(:retrying, notification: notification, level: :warn, deliver_after: '2026-08-20 16:21:36')
    end

    it 'quotes the app name when it contains whitespace' do
      obj = klass.new(double(name: 'My App'))
      expect(logger).to receive(:info).with('event=delivered rpush_notification_id=42 app="My App"')
      obj.log_push_event(:delivered, notification: notification)
    end

    it 'collapses newlines in field values so the record stays one line' do
      obj = klass.new(app)
      expect(logger).to receive(:error).with('event=connection_error app=my_app error="a b"')
      obj.log_push_event(:connection_error, level: :error, error: "a\nb")
    end

    it 'omits fields whose value is nil' do
      obj = klass.new(app)
      expect(logger).to receive(:info).with('event=delivered rpush_notification_id=42 app=my_app')
      obj.log_push_event(:delivered, notification: notification, device_token: nil)
    end
  end
end
