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

  it 'sends only admitted notifications and records each refusal without retrying it' do
    admitted = create_notification('admitted-token')
    refused = create_notification('refused-token')
    Rpush.config.admission = ->(n) { n.device_token == 'admitted-token' || :membership_retired }

    Rpush.push
    Rpush.push # a second pass finds nothing pending to resend

    expect(requests.map { |body| body.dig('message', 'token') }).to eq(['admitted-token'])
    expect(admitted.reload.delivered).to be(true)
    refused.reload
    expect([refused.delivered, refused.failed, refused.error_code, refused.error_description, refused.retries])
      .to eq([false, true, nil, 'Refused before send: membership_retired', 0])
  end

  it 'makes no network call when the hook raises' do
    notification = create_notification('any-token')
    Rpush.config.admission = ->(_) { raise 'admission store unavailable' }

    Rpush.push

    expect(requests).to be_empty
    expect(notification.reload.error_description).to eq('Refused before send: admission_error')
  end

  it 'leaves delivery unchanged without a hook' do
    create_notification('legacy-token')
    Rpush.push
    expect(requests.size).to eq(1)
  end

  context 'with the redis client', if: redis? do
    it 'carries the admission reference through the store but never onto the wire' do
      create_notification('bound-token', admission_ref: 'admission-ref-9f3c')
      seen = []
      Rpush.config.admission = ->(n) { seen << n.admission_ref; true }

      Rpush.push

      expect(seen).to eq(['admission-ref-9f3c'])
      expect(requests.size).to eq(1)
      expect(requests.first.to_json).not_to include('admission-ref-9f3c')
    end
  end
end
