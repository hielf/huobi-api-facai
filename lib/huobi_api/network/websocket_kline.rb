require 'zlib'
require_relative './websocket_base'

module HuobiApi
  module Network
    module WebSocket
      class KLine
        attr_accessor :sub_ws_pool, :req_ws_pool, :rt_kline_queue, :req_kline_queue

        def initialize
          # 保存接收到的K线数据(实时的或一次性请求的)
          @rt_kline_queue = []
          @req_kline_queue = []

          # ws连接池，池中的ws连接均已经处于open状态
          # sub_ws_pool连接池：
          #   用于订阅实时K线数据，每个ws连接订阅一部分币的实时K线，无需从池中pop
          # req_ws_pool连接池：
          #   用于一次性请求，每次从池中pop取出一个ws连接，请求完一次后放回池中
          @sub_ws_pool = []
          @sub_ws_pool_size = 20 # 目前ws连接池的大小为20，可增大
          @req_ws_pool = []
          @req_ws_pool_size = 32 # 目前ws连接池的大小为32，可增大
        end

        # @param type: 'sub' or 'req'
        def new_ws(url, type)
          return nil unless %w[sub req].include? type

          ws = WebSocket::new_ws(url)
          ws.on(:open) { |event| self.on_open(event, type) }
          ws.on(:close) { |event| self.on_close(event, type) }
          ws.on(:error) { |event| self.on_error(event, type) }
          ws.on(:message) { |event| self.on_message(event, type) }
          ws
        end

        # 初始化一次性请求价格K线的WS连接池
        def init_req_ws_pool(pool_size = @req_ws_pool_size)
          pool_size.times do
            ws = new_ws(WS_URLS[1] + '/ws', 'req')
            EM.schedule {
              timer = EM::PeriodicTimer.new(0.05) do
                if Utils.ws_opened?(ws)
                  req_ws_pool.push(ws)
                  timer.cancel
                end
              end
            }
          end
        end

        # 初始化实时价格K线的WS连接池
        def init_sub_ws_pool(pool_size = @sub_ws_pool_size)
          pool_size.times do
            ws = new_ws(WS_URLS[3] + '/ws', 'sub')
            EM.schedule {
              timer = EM::PeriodicTimer.new(0.05) do
                if Utils.ws_opened?(ws)
                  sub_ws_pool.push(ws)
                  timer.cancel
                end
              end
            }
          end
        end

        def init_ws_pool
          init_req_ws_pool
          init_sub_ws_pool
        end

        # 关闭连接池
        # @param pool_type: sub(@sub_ws_pool)或req(@req_ws_pool)
        def close_ws_pool(pool_type = 'req')
          pool = eval "#{pool_type}_ws_pool"
          pool.each { |ws| ws.close!(3001, "close and clear #{pool_type} ws pool") }
          pool.clear
        end

        # options: { type: 'realtime' }
        #          { type: "1min", from: xxx, to: xxx} 其中from和to可选
        # 注：
        #  - 1.默认情况下，指定to不指定from时，从to向前获取300根K线
        #  - 2.默认情况下，指定from不指定to时，获取最近的300根K线，等价于from未生效
        #  - 3.默认情况下，不指定from和to时，获取最近的300根K线
        #  - 官方说明一次性最多只能获取300根K线，但实际上可以获取更多，比如600根、900根
        # 对于某类型K线起始时间点t1和下一根K线起始时间点t2来说：
        #   - from == t1时，从t1开始请求，from > t1 时，从t2开始请求
        #   - t2 > to >= t1时，将获取到t1为止(包含t1)
        def gen_req(symbol, **options)
          case options
          in { type: 'realtime' }
            JSON.dump({ sub: "market.#{symbol}.kline.1min", id: symbol })
          in { type: '1min' | '5min' | '15min' | '30min' | '60min' | '1day' | '1week' => type }
            h = { req: "market.#{symbol}.kline.#{type}", id: symbol }

            # 官方给定的from和to的范围：[1501174800, 2556115200]
            if options[:from]
              h[:from] = (options[:from] < 1501174800 ? 1501174800 : options[:from])
            end

            if options[:to]
              h[:to] = (options[:to] > 2556115200 ? 2556115200 : options[:to])
            end

            # 如果只有from没有to，手动补齐to(获取最多900根K线)
            # 注：
            #   - 如果from是K线起始点，直接 from + N * distance(type) 会获得N+1根K线
            #   - 如果from不是K线起始点，直接 from + N * distance(type) 会获得N根K线
            if h[:from] and h[:to].nil?
              if h[:from] % distance(type) == 0
                t = h[:from] + 899 * distance(type)
              else
                t = h[:from] + 900 * distance(type)
              end

              h[:to] = (t > 2556115200 ? 2556115200 : t)
            end

            # 如果只有 to 没有 from，手动补齐from(获取最多900根K线)
            # 注：
            #   - 如果to是K线起始点， 直接 to - N * distance(type) 会获得N+1根K线
            #   - 如果to不是K线起始点，直接 to - N * distance(type) 会获得N根K线
            if h[:to] and h[:from].nil?
              if h[:to] % distance(type) == 0
                t = h[:to] - 899 * distance(type)
              else
                t = h[:to] - 900 * distance(type)
              end

              h[:from] = (t < 1501174800 ? 1501174800 : t)
            end

            JSON.dump(h)
          else
            raise "#{self.class}##{__method__.to_s}: argument wrong"
          end
        end

        private def distance(type)
          case type
          when '1min'; 60 # 60
          when '5min'; 300 # 5 * 60
          when '15min'; 900 # 15 * 60
          when '30min'; 1800 # 30 * 60
          when '60min'; 3600 # 60 * 60
          when '1day'; 86400 # 24 * 60 * 60
          when '1week'; 604800 # 7 * 24 * 60 * 60
          end
        end

        def send_req(ws, req)
          ws.send(req)
          ws.reqs.push(req)
        end

        # 订阅实时K线数据
        # ws要求已经处于Open状态
        def sub_kline(ws, symbol)
          # req = { sub: "market.#{symbol}.kline.1min", id: symbol })
          req = gen_req(symbol, type: 'realtime')
          ws.send(req)
          ws.reqs.push(req)
        end

        # 检查某币是否订阅了实时K线数据
        def subbed?(symbol)
          @sub_ws_pool.any? { |ws| ws.reqs.any? { |req| req[:id] == symbol } }
        end

        # 请求一次性请求K线数据
        # ws要求已经处于Open状态
        def req_kline(ws, symbol, **options)
          case options
          in { type: '1min' | '5min' | '15min' | '30min' | '60min' | '1day' | '1week' }
          else
            raise "#{self.class}#req_kline: argument wrong"
          end

          req = gen_req(symbol, **options)
          ws.send(req)
          ws.req = req
        end

        # 订阅某些指定币的实时K线数据
        def sub_coins_kline(coins)
          coins = Array[*coins]

          EM.schedule do
            coins.each_slice(coins.size / @sub_ws_pool_size + 1).each_with_index do |some_coins, idx|
              timer = EM::PeriodicTimer.new(0.1) do
                if (ws = @sub_ws_pool[idx])
                  some_coins.each { |symbol| sub_kline(ws, symbol) }
                  timer.cancel
                end
              end
            end
          end
        end

        # req_coins_kline(coins, type: '1min')
        # req_coins_kline(coins, type: '5min')
        # req_coins_kline(coins, type: '1min', from: xxx, to: xxx)
        def req_coins_kline(coins, **options)
          coins = Array[*coins]

          EM.schedule do
            coins.each do |symbol|
              tick_loop = EM.tick_loop do
                if (ws = @req_ws_pool.shift)
                  req_kline(ws, symbol, **options)
                  tick_loop.stop
                end
              end
            end
          end
        end

        def req_klines_by_reqs(reqs)
          x = ->(some_reqs) {
            while (req = some_reqs.pop)
              while true
                if (ws = @req_ws_pool.shift)
                  send_req(ws, req)
                  break
                end
                sleep 0.05
              end
            end
          }

          cnt = @req_ws_pool.size
          reqs.each_slice(cnt).each do |some_reqs|
            x.call(some_reqs)

            while @req_ws_pool.size < cnt / 2
              sleep 0.1
            end
          end
        end

        private

        def on_open(event, type)
          ws = event.current_target
          Log.debug(self.class) { "ws #{type} connected(#{ws.url})" }
        end

        def on_close(event, type)
          ws = event.current_target
          Log.debug(self.class) { "ws #{type} connection closed(#{ws.url}), #{event.reason}" }
          # websocket被关闭，重连
          self.ws_reconnect(ws, type) unless ws.force_close_flag
        end

        def on_error(event, type)
          ws = event.current_target
          Log.debug(self.class) { "ws #{type} connection error(#{ws.url}), #{event.message}" }
          # 创建websocket连接出错，重连
          self.ws_reconnect(ws, type) unless ws.force_close_flag
        end

        def ws_reconnect(old_ws, type)
          Log.debug(self.class) { "ws #{type} reconnect: #{old_ws.url}" }

          # 先移除ws
          pool = type == "sub" ? @sub_ws_pool : @req_ws_pool
          pool.delete_if { |w| w.uuid == old_ws.uuid }

          # 创建新的ws，并等待其open之后加入到ws池中
          ws = new_ws(old_ws.url, type)
          EM.schedule do
            timer = EM::PeriodicTimer.new(0.1) do
              if Utils.ws_opened?(ws)
                if type == 'sub'
                  ws.reqs = old_ws.reqs
                  ws.reqs.each { |req| ws.send(req) }
                  pool.push(ws)
                elsif type == 'req'
                  ws.req = old_ws.req
                  if ws.req # 如果ws上有请求，说明是正在请求中断开，应重发请求，接收数据后将自动放入连接池
                    ws.send(ws.req)
                  else
                    # 如果ws上没有请求，说明该ws处于空闲时断开，直接放进连接池
                    pool.push(ws)
                  end
                end
                timer.cancel
              end
            end
          end
        end

        def on_message(event, _type)
          ws = event.current_target
          blob_arr = event.data
          data = JSON.parse(Zlib::gunzip(blob_arr.pack('c*')), symbolize_names: true)
          if (ts = data[:ping])
            Utils.ws_opened?(ws) && ws.send(JSON.dump({ "pong": ts }))
          else
            handle_message(data, ws)
          end
        end

        def handle_message(data, ws)
          case data
          in { ch: _, tick: _ } # 有tick字段，说明是订阅后推送的实时K线数据
            # handle_realtime_data(data)
            @rt_kline_queue.push(data)
          in { id: _, rep: _, status: 'ok', data: Array } # 有rep字段，说明是一次性请求的K线数据
            # handle_oneshot_req_data(data)
            @req_kline_queue.push(data)
            ws.req = nil # 收到数据后，移除ws上的req
            @req_ws_pool.push(ws) # 将ws重新放回ws连接池
          in { status: 'ok' } # 可能是订阅成功、取消订阅成功的响应信息
            Log.debug(self.class) { "#{data.slice(:id, :status, :subbed)}" }
            # {:id=>"gtusdt", :status=>"ok", :subbed=>"market.gtusdt.kline.1min", :ts=>1621512734085}
          in { status: 'error' }
            # {
            #   :status=>"error", :ts=>1621517393030, :id=>"nftusdt", :"err-code"=>"bad-request",
            #   :"err-msg"=>"symbol:nftusdt trade not open now "
            # }
            Log.error(self.class) { "error msgs: #{data}" }
          else
            Log.info(self.class) { "other msgs: #{data}" }
          end
        end
      end
    end
  end
end


