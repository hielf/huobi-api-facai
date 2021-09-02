require_relative './websocket_base'

module HuobiApi
  module Network
    module WebSocket
      # Order Tracker or Account Tracker
      class Trackers
        attr_accessor :ws
        # 保存交易订单的信息，包括：挂单、部分交易、交易完成、撤单
        attr_reader :orders
        # 订单监控队列，交易订单以及策略委托的信息都推送到该队列
        # 但注意，策略委托的请求应手动查询后推送到该队列，
        # 订单更新的websocket并不会推送策略委托的委托请求
        attr_reader :monitor_orders

        def initialize
          @url = WS_URLS[0] + '/ws/v2'
          # id: order_id 或 client_order_id
          @orders = Hash.new { |orders, id| orders[id] = [] }
          @monitor_orders = []

          @ws = init_ws(@url)
        end

        def init_ws(url = @url)
          cbs = {
            on_open: self.method(:on_open),
            on_close: self.method(:on_close),
            on_error: self.method(:on_error),
            on_message: self.method(:on_message)
          }
          WebSocket::new_ws(url, **cbs)
        end

        def sub_channel(ws, symbol)
          sub_req = { action: 'sub', ch: "orders##{symbol}" }
          ws.send(MultiJson.dump(sub_req))
          ws.tracker_reqs[symbol.to_sym] = sub_req
          ws
        end

        def sub_coin_channel(symbol)
          ws.wait_authed do |ws|
            sub_channel(ws, symbol)
          end
        end

        # @param symbols: symbol数组
        def sub_coins_channel(symbols)
          unless Array === symbols
            raise "#{self.class}##{__method__.to_s}: argument wrong"
          end

          # 订阅屏障，只有订阅屏障不存在时(false时)，才允许订阅
          # 当存在订阅屏障时，将异步等待，直到屏障被移除
          barrier = false

          wait_sub = nil

          # 为避免一次性订阅大量币时的大量失败，该lambda每轮只订阅少量(如一次性只订阅30个币)
          # 如果有大量币要订阅，应symbols.each_slice(30) {|coins| sub_reqs.call(coins)}
          sub_reqs = lambda do |some_coins|
            EM.schedule do
              # 间隔检查订阅屏障，存在屏障时，等待屏障被移除
              t = EM::PeriodicTimer.new(0.05) do
                unless barrier
                  t.cancel
                  self.ws.error_reqs = 0 # 每次发送请求都重置订阅错误数
                  self.ws.tracker_reqs = {} # 每次发送请求都清空之前所保存的请求
                  some_coins.each { |symbol| sub_channel(self.ws, symbol) }

                  # 发送订阅请求后，立起屏障阻挡其他订阅，并监控本轮订阅的状态
                  barrier = true
                  wait_sub.call
                end
              end
            end
          end

          # 检查每轮sub_reqs订阅部分币时的订阅状态
          # 如果本轮所有订阅都成功，则移除订阅屏障
          # 如果有失败的订阅，则重发这些失败的订阅请求
          wait_sub = lambda do
            EM.schedule do
              timer = EM::PeriodicTimer.new(0.05) do
                # 已经成功订阅本阶段的所有请求
                if self.ws.tracker_reqs.empty?
                  barrier = false
                  timer.cancel
                end

                # 本阶段所有订阅已全部回应，但有失败的订阅
                # 重新请求这些失败的订阅
                if self.ws.error_reqs != 0 and self.ws.error_reqs == self.ws.tracker_reqs.size
                  timer.cancel # 重发请求后，取消本轮状态检查定时器，重发后会重新调用一次状态检查
                  barrier = false # 暂时移除屏障，使能重发订阅请求，或使其他sub_reqs订阅能继续
                  sub_reqs.call(self.ws.tracker_reqs.keys)
                end
              end
            end
          end

          ws.wait_authed do |ws|
            symbols.each_slice(40) { |some_coins| sub_reqs.call(some_coins) }
          end
        end

        # 检查是否订阅了某币的订单更新通道
        def subbed?(symbol)
          ws && ws.reqs.any? { |req| req[:ch].split("#")[-1] == symbol }
        end

        # 检查订阅成功的数量
        def subbed_count
          ws && ws.reqs.size
        end

        private

        def on_open(event)
          Log.debug(self.class) { "Tracker connection opened" }
          ws = event.current_target
          ws.send(Utils.ws_auth_token(URI(ws.url).host))
        end

        # 注意，on_close、on_open、on_error、on_message事件触发后的任务都被追加到EM.reactor_thread中运行，
        # 因此重建ws连接时可能的阻塞操作(如sleep、Async task block)将导致EM的某些任务无法调度。
        # 为了避免可能的阻塞，直接在新线程中运行ws_reconnect任务
        def on_close(event)
          Log.debug(self.class) { "Tracker connection closed: #{event.reason}" }

          # 重建连接，并重新订阅已订阅过的请求
          ws = event.current_target
          reqs = ws.reqs.map { |msg| msg[:ch].split("#")[-1] }

          # 强制关闭连接，不重新订阅
          if ws.force_close_flag
            @ws = nil
            return
          end

          # 每10秒检查一次订阅情况，只要没订阅成功，就一直订阅，直到订阅成功
          @ws = nil
          Thread.new do
            while true
              break if @ws and @ws.reqs.size == reqs.size

              @ws&.close!
              @ws = init_ws(@url)
              sub_coins_channel(reqs)
              sleep 30
            end
          end
        end

        def on_error(event)
          Log.debug(self.class) { "Tracker connection error: #{event.message}" }
        end

        def on_message(event)
          ws = event.current_target
          msg = MultiJson.load(event.data, symbolize_keys: true)

          case msg
          in { action: "push", ch: /orders#.*usdt/, data: }
            # 普通订单的数据写入orders
            # 普通订单的数据写入监控队列monitor_orders
            # 策略委托撤单(trigger)和委托失败(deletion)的数据相关写入监控队列monitor_orders
            # 策略委托的委托请求数据(需手动查询得到)写入监控队列monitor_orders

            # 交易订单更新信息，不保存策略委托的委托请求
            # @orders[data[:orderId]].push(data) unless /^(?:trigger|deletion)$/.match?(data[:eventType])
            @orders[data[:orderId] || data[:clientOrderId]].push(data)
            @monitor_orders.push(data)

            # puts "#{order_id}: #{data}"
          in { action: "ping", data: { ts: } }
            pong_msg = MultiJson.dump({ action: 'pong', data: { ts: ts } })
            ws.send(pong_msg)
          in { ch: "auth", code: }
            # 认证响应的信息
            if code != 200
              Log.error(self.class) { 'Tracker websocket权限认证失败' }
              ws.close
              return
            end
            ws.authed = true
          in { action: "sub" => action, ch: /orders#.*usdt/ => ch, code: 200 }
            # 这是订阅订单更新的响应信息
            ws.reqs << msg.slice(:action, :ch)
            ws.tracker_reqs.delete(ch.split("#")[-1].to_sym)
            Log.debug(self.class) { "<#{ch}> #{action}scribed Trackers" }
          in { action: "unsub" => action, ch: /orders#.*usdt/ => ch, code: 200 }
            # 这是退订的响应消息
          in { code: 4000, message: 'too.many.request' }
            # 记录失败的请求数
            ws.error_reqs += 1
          else
            Log.error(self.class) { "Tracker msg else:, #{msg}" }
          end
        end
      end
    end
  end
end
