require 'zlib'
require_relative './websocket_base'

module HuobiApi
  module Network
    module WebSocket
      class KLine
        attr_accessor :sub_ws_pool, :req_ws_pool, :rt_kline_queue, :req_kline_queue

        def initialize
          # @coin_prices = Hash.new do |hash, symbol|
          #   hash[symbol] = {
          #     rt_price: [], half_sec: [], sec: [], "5sec": [],
          #     "1min": [], "5min": [], "15min": [], "30min": [],
          #     "60min": [], "1day": [], "1week": []
          #   }
          # end

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

        def init_ws_pool
          @req_ws_pool_size.times do
            ws = new_ws(WS_URLS[1] + '/ws', 'req')
            EM.schedule {
              timer = EM::PeriodicTimer.new(0.1) do
                if Utils.ws_opened?(ws)
                  @req_ws_pool.push(ws)
                  timer.cancel
                end
              end
            }
          end

          @sub_ws_pool_size.times do
            ws = new_ws(WS_URLS[3] + '/ws', 'sub')
            EM.schedule {
              timer = EM::PeriodicTimer.new(0.1) do
                if Utils.ws_opened?(ws)
                  @sub_ws_pool.push(ws)
                  timer.cancel
                end
              end
            }
          end
        end

        # options: { type: 'realtime' }
        #          { type: "1min", from: xxx, to: xxx} 其中from和to可选
        def gen_req(symbol, **options)
          case options
          in { type: 'realtime' }
            JSON.dump({ sub: "market.#{symbol}.kline.1min", id: symbol })
          in { type: '1min' | '5min' | '15min' | '30min' | '60min' | '1day' | '1week' => type }
            h = { req: "market.#{symbol}.kline.#{type}", id: symbol }
            h[:from] = options[:from] if options[:from]
            h[:to] = options[:to] if options[:to]
            JSON.dump(h)
          else
            raise "#{self.class}#gen_req}: argument wrong"
          end
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
          @sub_ws_pool.any? {|ws| ws.reqs.any? {|req| req[:id] == symbol } }
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

        private

        def on_open(event, type)
          ws = event.current_target
          Log.debug(self.class) { "ws #{type} connected(#{ws.url})" }
        end

        def on_close(event, type)
          ws = event.current_target
          Log.debug(self.class) { "ws #{type} connection closed(#{ws.url}), #{event.reason}" }
          # websocket被关闭，1秒后重连
          self.ws_reconnect(ws, type)
        end

        def on_error(event, type)
          ws = event.current_target
          Log.debug(self.class) { "ws #{type} connection error(#{ws.url}), #{event.message}" }
          # 创建websocket连接出错，1秒后重连
          self.ws_reconnect(ws, type)
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
          in { ch: _, tick: _} # 有tick字段，说明是订阅后推送的实时K线数据
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

        # # 一次性请求的数据直接保存即可
        # def handle_oneshot_req_data(data)
        #   symbol = data[:id].to_sym
        #   period = data[:req].split(".")[-1].to_sym
        #   @coin_prices[symbol][period] = data[:data]
        # end
        #
        # # 实时价格需保存到rt_price数组，且需实时维护K线数据
        # def handle_realtime_data(data)
        #   symbol = data[:ch].split(".")[1].to_sym
        #   tick = data[:tick]
        #
        #   rt_price = tick[:close]
        # end
      end
    end
  end
end

