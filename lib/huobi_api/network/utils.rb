require 'uri'
require 'base64'
require 'openssl'
require 'json'

require_relative './../base'

module HuobiApi
  module Network
    module Utils
      def self.build_query(data)
        data.map do |k, v|
          URI.encode_www_form_component(k) << '=' << URI.encode_www_form_component(v)
        end.join('&')
      end

      def self.hash_sort(hsh)
        Hash[hsh.sort_by { |k, _| k }]
      end

      # 签名：HmacSHA256算法计算后，再base64编码
      def self.sign(sign_data)
        Base64.encode64(OpenSSL::HMAC.digest('sha256', HuobiApi.secret_key, sign_data)).gsub("\n", '')
      end

      def self.rest_auth_token(method, host, path, req_data = nil)
        params = {
          'AccessKeyId' => HuobiApi.access_key,
          'SignatureMethod' => 'HmacSHA256',
          'SignatureVersion' => 2,
          'Timestamp' => Time.now.getutc.strftime('%Y-%m-%dT%H:%M:%S')
        }

        params = method == 'GET' && !req_data.nil? ? params.merge(req_data) : params

        sign_data = "#{method}\n#{host}\n#{path}\n#{build_query(hash_sort(params))}"
        params = params.merge({ 'Signature' => sign(sign_data) })
        params
      end

      # websocket鉴权
      def self.ws_auth_token(host)
        params = {
          'accessKey' => HuobiApi.access_key,
          'signatureMethod' => 'HmacSHA256',
          'signatureVersion' => 2.1,
          'timestamp' => Time.now.getutc.strftime('%Y-%m-%dT%H:%M:%S')
        }
        sign_data = "GET\n#{host}\n/ws/v2\n#{build_query(hash_sort(params))}"
        params = params.merge({ 'signature' => sign(sign_data), 'authType' => 'api' })
        JSON.dump({ action: 'req', ch: 'auth', params: params })
      end

      # ws是否处于打开状态
      def self.ws_opened?(ws)
        ws&.ready_state == Faye::WebSocket::OPEN
      end
    end
  end
end
