require_relative './websocket_base'

module HuobiAPI
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

        private

        def on_open(event)
          Log.debug(self.class) { "Tracker connection opened" }
          ws = event.current_target
          ws.send(Utils.ws_auth_token(URI(ws.url).host))
        end

        def on_close(event)
          Log.info(self.class) { "Tracker connection closed: #{event.reason}" }
        end

        def on_error(event)
          Log.error(self.class) { "Tracker connection error: #{event.message}" }
        end

        def on_message(event)
          ws = event.current_target
          msg = JSON.parse(event.data, symbolize_names: true)

          case msg
          in { action: "push", ch: /accounts.update/, data: }
            # 订阅账户余额变动后推送的消息
            return
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
          in { action: "sub" | "unsub" => action, ch: /orders#.*usdt/ => ch, code: 200 }
            # 这是订阅订单更新的响应信息
            Log.info(self.class) { "<#{ch}> #{action}scribed Trackers" }
          in { action: "sub", ch: /accounts.update/, code: 200 }
            # 这是订阅账余额变动的响应信息
            return
          else
            # {:code=>4000, :message=>"too.many.request"}
            Log.info(self.class) { "Tracker msg else:, #{msg}" }
          end
        end
      end
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  require_relative './../coins'

  t = Thread.new do
    coins = HuobiAPI::Coins.new

    EM.run do
      order_tracker = HuobiAPI::Network::WebSocket::Trackers.new
      tick_loop = EM.tick_loop { :stop if order_tracker.ws.authed? }
      tick_loop.on_stop do
        coins.all_symbols.each_slice(40).each_with_index do |some_coins, idx|
          EM.add_timer(idx) do
            some_coins.each do |symbol|
              sub_req = { action: 'sub', ch: "orders##{symbol}" }
              order_tracker.ws.send(JSON.dump(sub_req))
            end
          end
        end
      end

      EM.tick_loop do
        p order_tracker.orders.pop if order_tracker.orders.any?
      end
    end
  end
  sleep
end
