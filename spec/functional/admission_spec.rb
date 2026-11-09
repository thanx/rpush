require 'functional_spec_helper'

describe 'Pre-send admission' do
  let(:app) { Rpush::Fcm::App.new }
  let(:response) { double(Net::HTTPResponse, code: 200, body: { name: 'projects/x/messages/1' }.to_json) }
  let(:requests) { [] }
  let(:http) { double(Net::HTTP::Persistent, shutdown: nil) }
  let(:creds) { double(Google::Auth::UserRefreshCredentials) }

  before do
    app.name = 'admission'
    app.auth_key = 'abc123'
    app.save!

    allow(http).to receive(:request) do |_uri, post|
      requests << JSON.parse(post.body)
      response
    end
    allow(Net::HTTP::Persistent).to receive_messages(new: http)
    allow(creds).to receive(:fetch_access_token).and_return('access_token' => 'fake')
    allow(::Google::Auth::ServiceAccountCredentials).to receive(:make_creds).and_return(creds)
    allow_any_instance_of(::Rpush::Daemon::Fcm::Delivery).to receive(:necessary_data_exists?).and_return(true)
  end

  def create_notification(token, **attributes)
    notification = Rpush::Fcm::Notification.new(app_id: app.id, device_token: token, data: { message: 'hi' },
      **attributes)
    notification.save!
    notification
  end

  it 'sends admitted notifications and keeps refused notifications terminal across another daemon pass' do
    admitted = create_notification('admitted-token')
    refused = create_notification('refused-token')
    Rpush.config.admission = ->(n) { n.device_token == 'admitted-token' || :membership_retired }

    Rpush.push
    Rpush.push

    expect(requests.map { |body| body.dig('message', 'token') }).to eq(['admitted-token'])
    expect(admitted.reload.delivered).to be(true)
    refused.reload
    expect([refused.delivered, refused.failed, refused.error_code, refused.error_description, refused.retries])
      .to eq([false, true, nil, 'Refused before send: membership_retired', 0])
  end

  it 'makes no provider call when the hook raises' do
    notification = create_notification('any-token')
    Rpush.config.admission = ->(_) { raise 'admission store unavailable' }

    Rpush.push

    expect(requests).to be_empty
    expect(notification.reload.error_description).to eq('Refused before send: admission_error')
  end

  it 'preserves normal delivery without a hook' do
    create_notification('legacy-token')
    Rpush.push
    expect(requests.size).to eq(1)
  end

  context 'with the Redis client', if: redis? do
    it 'loads the stored reference without sending admission metadata to the provider' do
      create_notification('bound-token', admission_ref: 'admission-ref-9f3c')
      seen = []
      Rpush.config.admission = lambda do |notification|
        seen << notification.admission_ref
        notification.admission_context = { admission_id: 9, attempt_token: 'private-attempt-token' }
        true
      end

      Rpush.push

      expect(seen).to eq(['admission-ref-9f3c'])
      expect(requests.size).to eq(1)
      expect(requests.first.to_json).not_to include('admission-ref-9f3c', 'private-attempt-token')
    end
  end
end
