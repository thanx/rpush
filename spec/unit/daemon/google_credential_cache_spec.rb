require 'unit_spec_helper'

describe Rpush::Daemon::GoogleCredentialCache do
  subject { described_class.instance }

  let(:scope) { 'https://www.googleapis.com/auth/firebase.messaging' }
  let(:json_key_hash) do
    {
      'type'         => 'service_account',
      'project_id'   => 'curry-pizza-house',
      'private_key'  => "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
      'client_email' => 'fcm-push-sender@thanx-shared-infra.iam.gserviceaccount.com',
      'token_uri'    => 'https://oauth2.googleapis.com/token'
    }
  end
  let(:authorizer) { double(fetch_access_token: { 'access_token' => 'fake-token' }) }

  before do
    subject.instance_variable_set(:@credentials_cache, {})
  end

  describe '#access_token' do
    context 'when json_key is a Hash (matches production: rpush_apps.json_key is a native json column, which ActiveRecord deserializes to a Hash on read)' do
      it 'builds the credential stream from it instead of raising' do
        expect(Google::Auth::ServiceAccountCredentials).to receive(:make_creds) do |scope:, json_key_io:|
          expect(JSON.parse(json_key_io.read)).to eq(json_key_hash)
          authorizer
        end

        expect(subject.access_token(scope, json_key_hash)).to eq('access_token' => 'fake-token')
      end
    end

    context 'when json_key is already a String' do
      it 'still builds the credential stream correctly' do
        expect(Google::Auth::ServiceAccountCredentials).to receive(:make_creds) do |scope:, json_key_io:|
          expect(JSON.parse(json_key_io.read)).to eq(json_key_hash)
          authorizer
        end

        expect(subject.access_token(scope, json_key_hash.to_json)).to eq('access_token' => 'fake-token')
      end
    end
  end
end
