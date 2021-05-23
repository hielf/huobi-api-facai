require 'json'
require 'uri'
require 'net/http'
require 'logger'

require_relative './utils'
require_relative './../base'

module HuobiApi
  module Network
    module Rest
      @base_url = 'https://api.huobi.pro'
      # @base_url1 = 'https://api.hadax.com'
      # @base_url2 = 'https://api.huobipro.com'
      # @base_url3 = 'https://api-aws.huobi.pro'   # 似乎不可用？
      @http = nil

      def self.http
        init_rest_http_connection
      end

      def self.init_rest_http_connection
        return @http if @http&.started?

        uri = URI(@base_url)
        http = Net::HTTP.new(uri.host, uri.port, HuobiApi.proxy_addr, HuobiApi.proxy_port)
        http.use_ssl = true

        def http.base_url
          (self.use_ssl? ? 'https://' : 'http://') + self.address
        end

        @http = http.start
      end

      # @param method：查询方法，不区分大小写，如'get' 'POST'
      # @param path：查询路径，如'/v1/account/accounts'
      # @param req_data：一个hash，可是字符串格式的key，也可是symbol格式的key
      #   req_data = {symbol: 'btcusdt', 'account-id': 123}
      #   req_data = {'symbol' => 'btcusdt', 'account-id' => 123}
      def self.send_req(method, path, req_data = nil)
        http = self.http  # 取得http连接

        method = method.upcase
        headers = {
          'Content-Type' => 'application/json',
          'Accept' => 'application/json',
          'Accept-Language' => 'zh-CN',
          'User-Agent' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        }
        req_data = req_data&.transform_keys!(&:to_s)
        params = Utils.rest_auth_token(method, http.address, path, req_data)
        url = "#{http.base_url}#{path}?#{Utils.build_query(params)}"

        begin
          # get请求只需要第二个参数，参数都已经放入了url中
          # post请求才需要第三个参数，参数都在req_data中
          res = JSON.parse http.send_request(method, url, JSON.dump(req_data), headers).body
          res
        rescue StandardError => e
          { 'message' => 'error', 'request_error' => e.message }
        end
      end
    end
  end
end
