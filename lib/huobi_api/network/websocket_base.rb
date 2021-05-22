require 'faye/websocket'
require 'json'
require 'securerandom'

require_relative './../base'
require_relative './utils'

module HuobiApi
  module Network
    module WebSocket
      WS_URLS = [
        'wss://api.huobipro.com', # 适合一次性请求和订阅
        'wss://api.hadax.com', # 速度较慢
        'wss://api.huobi.pro',
        'wss://api-aws.huobi.pro'
      ].freeze

      # 为ws对象提供一些额外的属性
      module WS_Extend
        # authed：记录ws是否已完成认证(订阅账户余额更新和订单更新时，需认证，行情数据无需认证)
        # uuid: 为ws对象都设置一个uuid属性
        # req：一次性请求行情数据时，保存该ws上最近发出的请求，请求成功后(接收到数据了)，设置ws.req = nil
        # reqs: 订阅行情数据时，保存该ws上曾经订阅过的所有请求，使得关闭连接重连时可继续订阅这些币的实时K线
        attr_accessor :authed, :uuid, :req, :reqs

        def self.extended(target_ws)
          target_ws.uuid = SecureRandom.uuid
          target_ws.reqs = []
        end

        # ws认证通过之后，应将该属性设置为true
        def authed?
          !!@authed
        end
      end

      # 创建新的websocket连接，可直接在参数上指定回调，也可以返回ws之后指定回调
      # new_ws(@url, {on_open: on_open, on_close: on_close, on_error: on_err, on_message: on_msg})
      # 或
      # ws = new_ws; ws.on(:open) {}
      def self.new_ws(url, **cbs)
        proxy = "http://#{PROXY_ADDR}:#{PROXY_PORT}"
        ws = Faye::WebSocket::Client.new(url, [], { proxy: { origin: proxy } })
        ws.extend WS_Extend

        cbs[:on_open] && ws.on(:open) { |event| cbs[:on_open].call(event) }
        cbs[:on_close] && ws.on(:close) { |event| cbs[:on_close].call(event) }
        cbs[:on_error] && ws.on(:error) { |event| cbs[:on_error].call(event) }
        cbs[:on_message] && ws.on(:message) { |event| cbs[:on_message].call(event) }

        ws
      end
    end
  end
end


