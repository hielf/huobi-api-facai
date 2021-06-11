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

          class << @sub_ws_pool
            attr_accessor :inited # 是否已初始化的标记
            attr_accessor :pool_size
          end

          @sub_ws_pool.pool_size = 20 # 目前ws连接池的大小为20，可增大

          @req_ws_pool = []

          class << @req_ws_pool
            attr_accessor :inited # 是否已初始化的标记
            attr_accessor :pool_size
          end

          @req_ws_pool.pool_size = 32 # 目前ws连接池的大小为32，可增大
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
        def init_req_ws_pool(pool_size = @req_ws_pool.pool_size)
          pool_size.times do
            ws = new_ws(WS_URLS[1] + '/ws', 'req')

            ws.wait_opened do |ws|
              req_ws_pool.push(ws)
              req_ws_pool.inited = true # 设置初始化标记
              req_ws_pool.pool_size = pool_size
            end
          end
        end

        # 初始化实时价格K线的WS连接池
        def init_sub_ws_pool(pool_size = @sub_ws_pool.pool_size)
          EM.schedule do
            pool_size.times do
              ws = new_ws(WS_URLS[3] + '/ws', 'sub')

              ws.wait_opened do |ws|
                sub_ws_pool.push(ws)
                sub_ws_pool.inited = true # 设置初始化标记
                sub_ws_pool.pool_size = pool_size
              end
            end
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
          pool.inited = false # 设置未初始化标记
        end

        # 等待从req连接池中取出一个ws
        # 可给定语句块，将在获取到ws后执行语句块(传递ws作为语句块参数)
        # 如果不给语句块，则阻塞等待后返回ws
        def get_ws_from_req_pool(size = @req_ws_pool.pool_size)
          pool = req_ws_pool
          init_req_ws_pool(size) unless pool.inited

          if block_given?
            EM.schedule do
              timer = EM::PeriodicTimer.new(0.01) do
                if pool.any?
                  yield pool.shift
                  timer.cancel
                end
              end
            end
            return
          end

          loop do
            if pool.any?
              return pool.shift
            end
            sleep 0.01
          end
        end

        # 等待连接池初始化完成
        # 可给定语句块，将在初始化完成后执行(传递pool作为语句块参数)
        # 如果不给定语句块，则阻塞等待后返回
        def wait_pool_init(pool_type = 'req')
          pool = eval "#{pool_type}_ws_pool"

          if block_given?
            EM.schedule do
              timer = EM::PeriodicTimer.new(0.01) do
                if pool.pool_size == pool.size
                  yield pool
                  timer.cancel
                end
              end
            end
            return
          end

          while true
            return if pool.pool_size == pool.size
            sleep 0.01
          end
        end

        # options: { type: 'realtime' }
        #          { type: "1min", from: xxx, to: xxx} 其中from和to可选
        # 注：对于一次性请求时
        #   - 1.默认情况下，指定to不指定from时，从to向前获取300根K线
        #   - 2.默认情况下，指定from不指定to时，获取最近的300根K线，等价于from未生效
        #   - 3.默认情况下，不指定from和to时，获取最近的300根K线
        #   - 官方说明一次性最多只能获取300根K线，但实际上可以获取更多，比如600根、900根
        #   - 本方法对默认行为做了改变，在没有同时指定from和to时，默认生成可获取900根K线的req
        # 对于某类型K线起始时间点t1和下一根K线起始时间点t2来说：
        #    - from == t1时，从t1开始请求，from > t1 时，从t2开始请求
        #    - t2 > to >= t1时，将获取到t1为止(包含t1)
        def gen_req(symbol, **options)
          type = options[:type]
          x = ->(**options) {
            hash = {}

            # 官方给定的from和to的范围：[1501174800, 2556115200]
            if options[:from]
              hash[:from] = (options[:from] < 1501174800 ? 1501174800 : options[:from])
            end

            if options[:to]
              hash[:to] = (options[:to] > 2556115200 ? 2556115200 : options[:to])
            end

            # 如果只有from没有to，手动补齐to(获取最多900根K线)
            # 注：
            #   - 如果from是K线起始点，直接 from + N * distance(type) 会获得N+1根K线
            #   - 如果from不是K线起始点，直接 from + N * distance(type) 会获得N根K线
            if hash[:from] and hash[:to].nil?
              if hash[:from] % distance(type) == 0
                t = hash[:from] + 899 * distance(type)
              else
                t = hash[:from] + 900 * distance(type)
              end

              hash[:to] = (t > 2556115200 ? 2556115200 : t)
            end

            # 如果只有 to 没有 from，手动补齐from(获取最多900根K线)
            # 注：
            #   - 如果to是K线起始点， 直接 to - N * distance(type) 会获得N+1根K线
            #   - 如果to不是K线起始点，直接 to - N * distance(type) 会获得N根K线
            if hash[:to] and hash[:from].nil?
              if hash[:to] % distance(type) == 0
                t = hash[:to] - 899 * distance(type)
              else
                t = hash[:to] - 900 * distance(type)
              end

              hash[:from] = (t < 1501174800 ? 1501174800 : t)
            end

            if hash[:from].nil? and hash[:to].nil?
              hash[:to] = Time.now.to_i
              if hash[:to] % distance(type) == 0
                t = hash[:to] - 899 * distance(type)
              else
                t = hash[:to] - 900 * distance(type)
              end

              hash[:from] = (t < 1501174800 ? 1501174800 : t)
            end

            return hash
          }

          case type
          in 'realtime'
            JSON.dump({ sub: "market.#{symbol}.kline.1min", id: symbol })
          in '1min' | '5min' | '15min' | '30min' | '60min' | '1day' | '1week'
            h = { req: "market.#{symbol}.kline.#{type}", id: symbol }
            h = h.merge(x.call(**options))
            JSON.dump(h)
          else
            raise "#{self.class}##{__method__.to_s}: argument wrong"
          end
        end

        # 查找某币的K线起始时间点(多数情况下也即该币上线交易的时间点)
        # @return [Integer] epoch
        def kline_start_at(symbol)
          queue = kline.req_kline_queue

          cnt = 900 # 一次性请求900根周K线

          # 发送并获取请求得到的K线数据
          get_res = ->(req) {
            ws = get_ws_from_req_pool
            kline.send_req(ws, req)

            while true
              sleep 0.05
              if (data = queue&.shift)
                return data[:data]
              end
            end
          }

          # 先找到从哪一周开始
          to = Time.now.to_i
          start_week_epoch = while true
                               req = kline.gen_req(symbol, type: '1week', to: to)

                               data = get_res.call(req)
                               if data.empty?
                                 to += cnt * distance('1week')
                               elsif data.size < cnt
                                 break data[0][:id]
                               elsif data.size == cnt
                                 to = data[0][:id] - 1
                               end
                             end

          # 再从起始周查找5min K线，找到起始点
          from = start_week_epoch
          start_epoch = while true
                          req = kline.gen_req(symbol, type: '5min', from: from)

                          data = get_res.call(req)
                          if data.empty?
                            from += cnt * distance('5min')
                          elsif data.size < cnt
                            break data[0][:id]
                          elsif data.size == cnt
                            from = data[-1][:id] + 1
                          end
                        end

          start_epoch
        end

        # 生成某币某个K线类型的所有请求(从最早的第一根K线到目前为止，每个请求获取900根K线)
        # @param type '1min', '5min', '15min', '30min', '60min', '1day', '1week'
        def gen_coin_all_reqs(symbol, type)
          start_time = kline_start_at(symbol)
          (start_time..Time.now.to_i)
            .step(900 * distance(type))
            .map do |from|
            gen_req(symbol, type: type, from: from)
          end
        end

        # 检查某币是否订阅了实时K线数据
        def subbed?(symbol)
          @sub_ws_pool.any? { |ws| ws.reqs.any? { |req| req[:id] == symbol } }
        end

        # 订阅某些指定币的实时K线数据
        def sub_coins_kline(coins)
          coins = Array[*coins]

          unless @sub_ws_pool.inited
            init_sub_ws_pool
            wait_pool_init('sub')
          end

          coins.each_slice(coins.size / sub_ws_pool.pool_size + 1).each_with_index do |some_coins, idx|
            ws = sub_ws_pool[idx]
            some_coins.each { |symbol| sub_kline(ws, symbol) }
          end
        end

        # req_coins_kline(coins, type: '1min')
        # req_coins_kline(coins, type: '5min')
        # req_coins_kline(coins, type: '1min', from: xxx, to: xxx)
        def req_coins_kline(coins, **options)
          coins = Array[*coins]

          coins.each do |symbol|
            ws = get_ws_from_req_pool
            req_kline(ws, symbol, **options)
          end
        end

        def req_klines_by_reqs(reqs)
          while (req = reqs.shift)
            ws = get_ws_from_req_pool
            send_req(ws, req)
          end
        end

        private def distance(type)
          case type
          when '1min';
            60 # 60
          when '5min';
            300 # 5 * 60
          when '15min';
            900 # 15 * 60
          when '30min';
            1800 # 30 * 60
          when '60min';
            3600 # 60 * 60
          when '1day';
            86400 # 24 * 60 * 60
          when '1week';
            604800 # 7 * 24 * 60 * 60
          end
        end

        # 发送给定的请求
        private def send_req(ws, req)
          ws.send(req)
          ws.reqs.push(req)
        end

        # 订阅实时K线数据
        # ws要求已经处于Open状态
        private def sub_kline(ws, symbol)
          # req = { sub: "market.#{symbol}.kline.1min", id: symbol })
          req = gen_req(symbol, type: 'realtime')
          ws.send(req)
          ws.reqs.push(req)
        end

        # 请求一次性请求K线数据
        # ws要求已经处于Open状态
        private def req_kline(ws, symbol, **options)
          case options
          in { type: '1min' | '5min' | '15min' | '30min' | '60min' | '1day' | '1week' }
          else
            raise "#{self.class}#req_kline: argument wrong"
          end

          req = gen_req(symbol, **options)
          ws.send(req)
          ws.req = req
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
          pool = eval "#{type}_ws_pool"
          pool.delete_if { |w| w.uuid == old_ws.uuid }

          # 创建新的ws，并等待其open之后加入到ws池中
          ws = new_ws(old_ws.url, type)

          ws.wait_opened do
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
          end
        end

        def on_message(event, _type)
          ws = event.current_target
          blob_arr = event.data
          data = JSON.parse(Zlib::gunzip(blob_arr.pack('c*')), symbolize_names: true)
          if (ts = data[:ping])
            ws.opened? && ws.send(JSON.dump({ "pong": ts }))
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


