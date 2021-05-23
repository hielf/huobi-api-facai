require_relative './websocket_base'

module HuobiApi
  module Network
    module WebSocket
      # Order Tracker or Account Tracker
      class Trackers
        attr_accessor :ws, :orders

        def initialize
          @url = WS_URLS[0] + '/ws/v2'
          # @orders = Hash.new {|hash, key| hash[key] = []}
          @orders = []

          on_open = self.method(:on_open)
          on_close = self.method(:on_close)
          on_error = self.method(:on_error)
          on_message = self.method(:on_message)
          cbs = {
            on_open: on_open,
            on_close: on_close,
            on_error: on_error,
            on_message: on_message
          }
          @ws = WebSocket::new_ws(@url, **cbs)
        end

        # @param symbol_code_or_req: 可以是单个symbol名称，也可以是该币的请求{action: 'sub', ch: "orders#btcusdt"}
        def sub_channel(ws, symbol_code_or_req)
          case symbol_code_or_req
          in { action: 'sub', ch: /^orders#.*usdt$/ }
            sub_req = symbol_code_or_req
            symbol = symbol_code_or_req[:ch].split('#')[-1]
          else
            sub_req = { action: 'sub', ch: "orders##{symbol_code_or_req}" }
            symbol = symbol_code_or_req
          end

          ws.send(JSON.dump(sub_req))
          ws.tracker_reqs[symbol.to_sym] = sub_req
          ws
        end

        # @param symbol_code_or_req: 可以是单个symbol名称，也可以是该币的请求{action: 'sub', ch: "orders#btcusdt"}
        def sub_coin_channel(symbol_code_or_req)
          EM.run do
            tick_loop = EM.tick_loop do
              if self.ws.authed?
                sub_channel(self.ws, symbol_code_or_req)
                tick_loop.stop
              end
            end
          end
        end

        # @param symbols_or_reqs: 可以是symbol数组，也可以是请求数组[{action: 'sub', ch: "orders#btcusdt"},...]或他们的混合
        def sub_coins_channel(symbols_or_reqs)
          unless Array === symbols_or_reqs
            raise "#{self.class}##{__method__.to_s}: argument wrong"
          end

          # 订阅屏障，只有订阅屏障不存在时(false时)，才允许订阅
          # 当存在订阅屏障时，将异步等待，直到屏障被移除
          barrier = false

          wait_sub = nil

          # 为避免一次性订阅大量币时的大量失败，该lambda每轮只订阅少量(如一次性只订阅30个币)
          # 如果有大量币要订阅，应symbols.each_slice(30) {|coins| sub_reqs.call(coins)}
          sub_reqs = lambda do |some_coins|
            EM.run do
              # 间隔检查订阅屏障，存在屏障时，等待屏障被移除
              t = EM::PeriodicTimer.new(0.1) do
                unless barrier
                  t.cancel
                  self.ws.error_reqs = 0  # 每次发送请求都重置订阅错误数
                  self.ws.tracker_reqs = {}  # 每次发送请求都清空之前所保存的请求
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
            EM.run do
              timer = EM::PeriodicTimer.new(0.1) do
                # 已经成功订阅本阶段的所有请求
                if self.ws.tracker_reqs.empty?
                  barrier = false
                  timer.cancel
                end

                # 本阶段所有订阅已全部回应，但有失败的订阅
                # 重新请求这些失败的订阅
                if self.ws.error_reqs != 0 and self.ws.error_reqs == self.ws.tracker_reqs.size
                  timer.cancel     # 重发请求后，取消本轮状态检查定时器，重发后会重新调用一次状态检查
                  barrier = false  # 暂时移除屏障，使能重发订阅请求，或使其他sub_reqs订阅能继续
                  sub_reqs.call(self.ws.tracker_reqs.keys)
                end
              end
            end
          end

          EM.run do
            tick_loop = EM.tick_loop do
              if self.ws.authed?
                symbols_or_reqs.each_slice(40) { |some_coins| sub_reqs.call(some_coins) }
                tick_loop.stop
              end
            end
          end
        end

        private

        def on_open(event)
          Log.debug(self.class) { "Tracker connection opened" }
          ws = event.current_target
          ws.send(Utils.ws_auth_token(URI(ws.url).host))
        end

        def on_close(event)
          Log.info(self.class) { "Tracker connection closed: #{event.reason}" }
          # 重建连接，并重新订阅已订阅过的请求
          ws = event.current_target
          reqs = ws.reqs
          ws.reqs = []
          sub_coins_channel(reqs)
        end

        def on_error(event)
          Log.error(self.class) { "Tracker connection error: #{event.message}" }
        end

        def on_message(event)
          ws = event.current_target
          msg = JSON.parse(event.data, symbolize_names: true)

          case msg
          in { action: "push", ch: /orders#.*usdt/, data: }
            # 订单变动后推送的信息
            @orders.push(data)
            # puts "#{order_id}: #{data}"
          in { action: "ping", data: { ts: } }
            pong_msg = JSON.dump({ action: 'pong', data: { ts: ts } })
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

if __FILE__ == $PROGRAM_NAME
  require_relative './../coins'

  _ = Thread.new do
    coins = HuobiApi::Coins.new.all_symbols

    EM.run do
      order_tracker = HuobiApi::Network::WebSocket::Trackers.new
      order_tracker.sub_coins_channel(coins)

      EM.tick_loop do
        p order_tracker.orders.pop if order_tracker.orders.any?
      end
    end
  end
  sleep
end
