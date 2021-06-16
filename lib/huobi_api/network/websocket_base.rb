require 'faye/websocket'
require 'json'
require 'securerandom'

require_relative './../base'
require_relative './utils'
require_relative './network_url'

module HuobiApi
  module Network
    module WebSocket
      # 尽早开启EM.reactor
      # 因为EM只能有一个事件循环reactor，因此不要再手动开启EM.run
      # 如果需要使用EM的功能，直接EM.schedule
      Thread.new { EM.run } unless EM.reactor_running?

      # 为ws对象提供一些额外的属性
      module WS_Extend
        # authed：记录ws是否已完成认证(订阅账户余额更新和订单更新时，需认证，行情数据无需认证)
        # uuid: 为ws对象都设置一个uuid属性
        # req：一次性请求行情数据时，保存该ws上最近发出的请求，请求成功后(接收到数据了)，设置ws.req = nil
        # reqs: 订阅订单更新或实时K线数据时，保存该ws上曾经成功订阅过的所有请求，使得关闭连接重连时可重新订阅
        # tracker_reqs: 订阅订单更新通道时使用，每请求订阅一个币，记录该币订阅记录，每订阅成功一个币，删除该币订阅记录
        # error_reqs：订阅订单更新通道时使用，每当订阅某币订单更新失败时，错误请求数+1
        attr_accessor :authed, :uuid, :req, :reqs, :tracker_reqs, :error_reqs
        attr_accessor :force_close_flag  # 设置该标记后，ws关闭时不会重建连接

        def self.extended(target_ws)
          target_ws.uuid = SecureRandom.uuid
          target_ws.reqs = []
          target_ws.tracker_reqs = {}
          target_ws.error_reqs = 0
          target_ws.force_close_flag = false
        end

        # ws认证通过之后，应将该属性设置为true
        def authed?
          !!@authed
        end

        # 等待认证完成
        # 需给定语句块，在等待完成后会被执行
        def wait_authed
          raise "missing block" unless block_given?
          EM.schedule do
            timer = EM::PeriodicTimer.new(0.01) do
              if authed?
                yield self
                timer.cancel
              end
            end
          end
        end

        # ws已open?
        def opened?
          self.ready_state == Faye::WebSocket::OPEN
        end

        # 等待ws进入open状态
        # 需给定语句块，在等待完成后会被执行
        def wait_opened
          # raise "missing block" unless block_given?

          if block_given?
            EM.schedule do
              timer = EM::PeriodicTimer.new(0.05) do
                if opened?
                  yield self
                  timer.cancel
                end
              end
            end
            return
          end

          Async do |subtask|
            subtask.sleep 0.05 until opened?
            self
          end.wait
        end

        # 强制关闭ws连接，不会重建连接
        def close!(code = nil, reason = nil)
          self.force_close_flag = true
          self.close(code, reason)
        end
      end

      # 创建新的websocket连接，可直接在参数上指定回调，也可以返回ws之后指定回调
      # new_ws(@url, {on_open: on_open, on_close: on_close, on_error: on_err, on_message: on_msg})
      # 或
      # ws = new_ws; ws.on(:open) {}
      def self.new_ws(url, **cbs)
        if HuobiApi.proxy
          ws = Faye::WebSocket::Client.new(url, [], { proxy: { origin: HuobiApi.proxy } })
        else
          ws = Faye::WebSocket::Client.new(url)
        end

        ws.extend WS_Extend

        # cbs.slice(*%i(on_close on_open on_error on_message)).each do |type, cb|
        #   ws.on(type[/_\K.*/].to_sym) {|event| cb.call(event)}
        # end
        cbs[:on_open] && ws.on(:open) { |event| cbs[:on_open].call(event) }
        cbs[:on_close] && ws.on(:close) { |event| cbs[:on_close].call(event) }
        cbs[:on_error] && ws.on(:error) { |event| cbs[:on_error].call(event) }
        cbs[:on_message] && ws.on(:message) { |event| cbs[:on_message].call(event) }

        ws
      end
    end
  end
end


