require 'faye/websocket'
require 'oj'
require 'json'
require 'multi_json'
require 'securerandom'
require 'async'

require_relative './../base'
require_relative './utils'
require_relative './network_url'

module HuobiApi
  module Network
    module WebSocket
      # 尽早开启EM.reactor
      # 因为EM只能有一个事件循环reactor，因此不要再手动开启EM.run
      # 如果需要使用EM的功能，直接EM.schedule
      # Thread.new { EM.run } unless EM.reactor_running?
      # Thread.pass until EventMachine.reactor_running?
      unless EM.reactor_running?
        Thread.new do
          # EM.run do
          #   # EM.add_shutdown_hook do
          #   #   # 定义一个EM退出时的标记
          #   #   def EM.exiting = true
          #   # end
          # end
          EM.run
        end
      end
      Thread.pass until EM.reactor_running?

      # 为ws对象提供一些额外的属性
      module WS_Extend
        # authed：记录ws是否已完成认证(订阅账户余额更新和订单更新时，需认证，行情数据无需认证)
        # uuid: 为ws对象都设置一个uuid属性
        # req：一次性请求行情数据时，保存该ws上最近发出的请求，请求成功后(接收到数据了)，设置ws.req = nil
        # reqs: 订阅订单更新或实时K线数据时，保存该ws上曾经成功订阅过的所有请求，使得关闭连接重连时可重新订阅
        # tracker_reqs: 订阅订单更新通道时使用，每请求订阅一个币，记录该币订阅记录，每订阅成功一个币，删除该币订阅记录
        # error_reqs：订阅订单更新通道时使用，每当订阅某币订单更新失败时，错误请求数+1
        attr_accessor :authed, :uuid, :req, :reqs, :tracker_reqs, :error_reqs
        attr_accessor :force_close_flag # 设置该标记后，ws关闭时不会重建连接
        # 标记该ws是否处于正在open，当ws成功open后，被设置为false
        attr_accessor :opening

        def self.extended(target_ws)
          target_ws.uuid = SecureRandom.uuid
          target_ws.reqs = []
          target_ws.tracker_reqs = {}
          target_ws.error_reqs = 0
          target_ws.force_close_flag = false
          target_ws.opening = true
        end

        # ws认证通过之后，应将该属性设置为true
        def authed?
          !!@authed
        end

        # 等待认证完成
        # 需给定语句块，在等待完成后会被执行
        def wait_authed
          t = Async(annotation: 'wait authed') do |task|
            task.sleep 0.05 until authed?
            next yield self if block_given?
            self
          end
          return t if block_given?
          t.wait
        end

        # CONNECTING: 0
        # OPEN: 1
        # CLOSING: 2
        # CLOSED: 3

        # ws已open?
        def opened?
          self.ready_state == Faye::WebSocket::OPEN
        end

        def closed?
          self.ready_state == Faye::WebSocket::CLOSED
        end

        # 等待ws进入open状态
        # 需给定语句块，在等待完成后会被执行
        def wait_opened
          t = Async(annotation: 'wait ws opened') do |subtask|
            n = 1
            until opened? or closed?
              subtask.sleep 0.05
              n += 1

              if n % 100 == 0
                Log.debug(self.class) {"wait ws open: (state: #{ready_state}/#{Faye::WebSocket::OPEN}), #{uuid}, #{req}"}
              end
              self.close if n % 400 == 0  # 如果等待20秒后还在等待，关掉连接
            end

            if closed?
              Log.debug(self.class) {"un-opened ws closed: #{uuid}, #{req}"}
              next
            end
            next yield self if block_given?
            self
          end

          return if closed?

          return t if block_given?
          t.wait
        end

        # 强制关闭ws连接，不会重建连接
        def close!(code = nil, reason = nil)
          self.force_close_flag = true
          self.close(code, reason)
        end

        def inspect
          "#<ws: {state: #{ready_state}}, #{url}, #{uuid}>"
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


