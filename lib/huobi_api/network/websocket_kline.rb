require 'zlib'
require_relative './websocket_base'
require_relative './websocket_wspool'

module HuobiApi
  module Network
    module WebSocket
      class BaseKLine
        attr_reader :ws_pool, :ws_pool_size, :ws_url, :queue

        def initialize
          # ws连接池，池中的ws连接均已经处于open状态
          @ws_pool = nil
          @ws_pool_size = nil
          @ws_url = nil

          # 存放订阅或接收后的K线数据
          @queue = []
        end

        # 初始化一次性请求价格K线的WS连接池
        private def create_ws_pool(pool_size, url, type)
          raise ArgumentError, "type must be 'req' or 'sub'" unless %w(req sub).include?(type)

          return if ws_pool

          cbs = ws_event_handlers(type)

          @ws_pool = WSPool.new(pool_size, url, **cbs)
          @ws_pool_size = pool_size
          @ws_url = url
          @ws_pool
        end

        private def ws_event_handlers(type)
          on_open = nil
          on_close = nil
          on_error = nil
          on_message = nil
          ws_reconnect = nil

          on_open = ->(event) {
            ws = event.current_target
            Log.debug(self.class) { "ws #{type} connected(#{ws.url})" }
          }

          on_close = ->(event) {
            ws = event.current_target
            Log.debug(self.class) { "ws #{type} connection closed(#{ws.url}), #{event.reason}" }
            # websocket被关闭，重连

            # 注意，on_close、on_open、on_error、on_message事件触发后的任务都被追加到EM.reactor_thread中运行，
            # 因此ws_reconnect中可能的阻塞操作(如sleep、Async task block)将导致EM的某些任务无法调度。
            # 为了避免可能的阻塞，直接在新线程中运行ws_reconnect任务
            Thread.new do
              ws_reconnect.call(ws) unless ws.force_close_flag # or EM.respond_to?(:exiting)
            end
          }

          on_error = ->(event) {
            ws = event.current_target
            Log.debug(self.class) { "ws #{type} connection error(#{ws.url}), #{event.message}" }
            # 创建websocket连接出错，重连

            # 使用新线程执行ws_reconnect的原因同上
            Thread.new do
              ws_reconnect.call(ws) unless ws.force_close_flag #or EM.respond_to?(:exiting)
            end
          }

          on_message = ->(event) {
            ws = event.current_target
            blob_arr = event.data
            data = JSON.parse(Zlib::gunzip(blob_arr.pack('c*')), symbolize_names: true)

            case data
            in { ping: ts }
              ws.opened? && ws.send(JSON.dump({ "pong": ts }))
            in { ch: _, tick: _ } # 有tick字段，说明是订阅后推送的实时K线数据
              # handle_realtime_data(data)
              @queue.push(data)
            in { id: _, rep: _, status: 'ok', data: Array } # 有rep字段，说明是一次性请求的K线数据
              # handle_oneshot_req_data(data)
              @queue.push(data)
              ws.req = nil # 收到数据后，移除ws上的req
              @ws_pool.push(ws) # 将ws重新放回ws连接池
            in { status: 'ok' } # 可能是订阅成功、取消订阅成功的响应信息
              Log.debug(self.class) { "#{data.slice(:id, :status, :subbed)}" }
              # {:id=>"gtusdt", :status=>"ok", :subbed=>"market.gtusdt.kline.1min", :ts=>1621512734085}
            in { status: 'error' }
              # {
              #   :status=>"error", :ts=>1621517393030, :id=>"nftusdt", :"err-code"=>"bad-request",
              #   :"err-msg"=>"symbol:nftusdt trade not open now "
              # }
              # {
              #   :status=>"error", :ts=>1623720217099, :id=>"nestusdt", :"err-code"=>"bad-request",
              #   :"err-msg"=>"429 too many request topic is market.nestusdt.kline.5min"
              # }
              Log.error(self.class) { "error msgs: #{data}" }
            else
              Log.info(self.class) { "other msgs: #{data}" }
            end
          }

          ws_reconnect = ->(old_ws) {
            Log.debug(self.class) { "ws #{type} reconnect: #{old_ws.url}" }

            # 先移除ws
            pool = ws_pool
            pool.delete_by_uuid(old_ws.uuid)

            # 创建新的ws，并等待其open之后加入到ws池中
            cbs = ws_event_handlers(type)

            ws = WebSocket.new_ws(old_ws.url, **cbs)

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
          }

          {
            on_open: on_open,
            on_close: on_close,
            on_error: on_error,
            on_message: on_message,
          }
        end
      end

      class ReqKLine < BaseKLine
        def initialize(pool_size = 32, url = nil)
          super()

          url = url || (WS_URLS[1] + '/ws')
          # ws连接池，池中的ws连接均已经处于open状态
          # 用于一次性请求，每次从池中pop取出一个ws连接，请求完一次后放回池中
          create_ws_pool(pool_size, url, 'req').wait_pool_init
        end

        # 1.默认情况下，指定to不指定from时，从to向前获取300根K线
        # 2.默认情况下，指定from不指定to时，获取最近的300根K线，等价于from未生效
        # 3.默认情况下，不指定from和to时，获取最近的300根K线
        # 官方说明一次性最多只能获取300根K线，但实际上可以获取更多，比如600根、900根
        # 本方法对默认行为做了改变，总是会补齐from和to，效果是：
        #       在没有同时指定from和to时，默认生成可获取900根K线的req
        # 对于某类型K线起始时间点t1和下一根K线起始时间点t2来说：
        #    - from == t1时，从t1开始请求，from > t1 时，从t2开始请求
        #    - t2 > to >= t1时，将获取到t1为止(包含t1)
        def gen_req(symbol, type, from: nil, to: nil)
          valid_types = %w[1min 5min 15min 30min 60min 1day 1week]
          unless valid_types.include?(type)
            raise ArgumentError, " invalid type: #{type}, valid types: #{valid_types}"
          end

          # 如果只有 to 没有 from，手动补齐from(获取最多900根K线)
          # 注：
          #   - 如果to是K线起始点， 直接 to - N * distance(type) 会获得N+1根K线
          #   - 如果to不是K线起始点，直接 to - N * distance(type) 会获得N根K线
          # 如果只有 from 没有 to，手动补齐to(获取最多900根K线)
          # 注：
          #   - 如果from是K线起始点，直接 from + N * distance(type) 会获得N+1根K线
          #   - 如果from不是K线起始点，直接 from + N * distance(type) 会获得N根K线
          # 如果既没有from，也没有to，则手动补齐获取最近的900根K线
          case [from, to]
          in [nil, nil | Integer]
            to = (to.nil? ? Time.now.to_i : to)
            from = to - (to % distance(type) == 0 ? 899 : 900) * distance(type)
          in [Integer, nil]
            to = from + (from % distance(type) == 0 ? 899 : 900) * distance(type)
          in [Integer, Integer]
          else
            raise ArgumentError, "invalid time range: #{[from, to]}"
          end

          # 官方给定的from和to的范围：[1501174800, 2556115200]
          JSON.dump({
                      req: "market.#{symbol}.kline.#{type}",
                      id: symbol,
                      from: from < 1501174800 ? 1501174800 : from,
                      to: to > 2556115200 ? 2556115200 : to
                    })
        end

        # 获取币的K线起始时间点(多数情况下也即该币上线交易的时间点)
        def kline_start_at(symbol)
          tmp_data = []
          cnt = 900 # 每次获取900根K线

          # 新建一个临时ws
          cbs = {
            on_open: ->(_event) { Log.debug(self.class) { "ws connected" } },
            on_close: ->(_event) { Log.debug(self.class) { "ws closed" } },
            on_error: ->(_event) {},
            on_message: ->(event) {
              ws = event.current_target
              blob_arr = event.data
              data = JSON.parse(Zlib::gunzip(blob_arr.pack('c*')), symbolize_names: true)

              case data
              in { ping: ts }
                ws.opened? && ws.send(JSON.dump({ "pong": ts }))
              in { id:, rep:, status: 'ok', data: Array }
                tmp_data << data
              else
                Log.debug(self.class) { "other msgs: #{data}" }
              end
            }
          }
          url = WS_URLS[0] + '/ws'
          ws = WebSocket.new_ws(url, **cbs).wait_opened

          get_res = ->(req) {
            ws.send(req)

            Async do |subtask|
              subtask.sleep 0.05 until tmp_data.any?
              tmp_data.shift[:data]
            end.wait
          }

          # 找到从哪一周开始(为了防止起始周前面还有K线数据，找到的起始周-1)
          find_week = ->(to = Time.now.to_i) {
            req = gen_req(symbol, '1week', to: to)
            data = get_res.call(req)
            if data.size < cnt
              data[0][:id] - distance('1week')
            elsif data.size == cnt
              to = data[0][:id] - 1
              find_week.call(to = to)
            end
          }

          # 找到起始周后，从起始周开始找到起始时间点
          find_min = ->(from) {
            req = gen_req(symbol, '30min', from: from)
            data = get_res.call(req)
            if data.empty?
              from += cnt * distance('30min')
              find_min.call(from)
            elsif data.size < cnt
              data[0][:id]
            elsif data.size == cnt
              from = data[-1][:id] + 1
              find_min.call(from)
            end
          }

          week_epoch = find_week.call
          start_time = find_min.call(week_epoch)
          ws.close
          start_time
        end

        # 查找一个或多个币的K线起始时间点
        # @param symbols 数组
        # @return {btcusdt: 12121212, ethusdt: 213232323,...}
        def klines_start_at(symbols)
          times = {}

          # 最大并发64个查询
          symbols.each_slice(64).each do |some_coins|
            Async do |task|
              some_coins.each do |symbol|
                Async do |subtask|
                  times[symbol] = kline_start_at(symbol)
                end
              end
            end
          end

          times
        end

        # 生成某币某个K线类型的所有请求(从最早的第一根K线到目前为止，每个请求获取900根K线)
        # @param type '1min', '5min', '15min', '30min', '60min', '1day', '1week'
        def gen_coin_all_reqs(symbol, type)
          cnt = 900
          start_time = kline_start_at(symbol)
          (start_time..Time.now.to_i)
            .step(cnt * distance(type))
            .map do |from|
            gen_req(symbol, type, from: from)
          end
        end

        def req_coins_kline(coins, type, from: nil, to: nil)
          coins = Array[*coins]

          coins.each do |symbol|
            ## async version
            # ws_pool.shift! do |ws|
            #   req_kline(ws, symbol, type, from: from, to: to)
            # end

            ## block version
            ws = ws_pool.shift!
            req_kline(ws, symbol, type, from: from, to: to)
          end
        end

        def req_klines_by_reqs(reqs)
          while (req = reqs.shift)
            ws = ws_pool.shift!
            send_req(ws, req)
          end
        end

        # 发送给定的请求
        private def send_req(ws, req)
          ws.send(req)
          ws.reqs.push(req)
        end

        # 请求一次性请求K线数据
        private def req_kline(ws, symbol, type, from: nil, to: nil)
          req = gen_req(symbol, type, from: from, to: to)
          ws.send(req)
          ws.req = req
        end

        private def distance(type)
          case type
          when '1min' then
            60 # 60
          when '5min' then
            300 # 5 * 60
          when '15min' then
            900 # 15 * 60
          when '30min' then
            1800 # 30 * 60
          when '60min' then
            3600 # 60 * 60
          when '1day' then
            86400 # 24 * 60 * 60
          when '1week' then
            604800 # 7 * 24 * 60 * 60
          end
        end
      end

      class SubKLine < BaseKLine
        def initialize
          super

          # ws连接池，池中的ws连接均已经处于open状态
          # 用于订阅实时K线数据，每个ws连接订阅一部分币的实时K线，无需从池中pop
          create_ws_pool(20, WS_URLS[3] + '/ws', 'sub').wait_pool_init
        end

        def gen_req(symbol)
          JSON.dump({ sub: "market.#{symbol}.kline.1min", id: symbol })
        end

        # 检查某币是否订阅了实时K线数据
        def subbed?(symbol)
          # reqs:
          # [
          #   "{\"sub\":\"market.sandusdt.kline.1min\",\"id\":\"sandusdt\"}",
          #   "{\"sub\":\"market.gtusdt.kline.1min\",\"id\":\"gtusdt\"}",
          # ]
          ws_pool.any? { |ws| ws.reqs.any? { |req| req.include?(symbol) } }
        end

        # 订阅某些指定币的实时K线数据
        def sub_coins_kline(coins)
          coins = Array[*coins]

          pool = ws_pool
          coins.each_slice(coins.size / pool.pool_size + 1).each_with_index do |some_coins, idx|
            ws = pool[idx]
            some_coins.each { |symbol| sub_kline(ws, symbol) }
          end
        end

        # 订阅实时K线数据
        private def sub_kline(ws, symbol)
          # req = { sub: "market.#{symbol}.kline.1min", id: symbol })
          req = gen_req(symbol)
          ws.send(req)
          ws.reqs.push(req)
        end
      end
    end
  end
end


