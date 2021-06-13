require_relative './websocket_base'

module HuobiApi
  module Network
    module WebSocket
      class WSPool
        attr_reader :url, :pool_size, :pool

        def initialize(pool_size = 32, url, **cbs)
          @url = url
          @pool_size = pool_size
          @pool = []

          init_ws_pool(pool_size, url, **cbs)
        end

        def new_ws(url, **cbs)
          begin
            cbs.fetch_values(:on_open, :on_close, :on_error, :on_message)
          rescue KeyError
            raise "argument cbs only supported: on_open, on_close, on_error, on_message"
          end

          ws = WebSocket::new_ws(url)
          ws.on(:open) { |event| cbs[:on_open].call(event) }
          ws.on(:close) { |event| cbs[:on_close].call(event) }
          ws.on(:error) { |event| cbs[:on_error].call(event) }
          ws.on(:message) { |event| cbs[:on_message].call(event) }
          ws
        end

        # 初始化WS连接池
        def init_ws_pool(pool_size = @pool_size, url, **cbs)
          EM.schedule do
            pool_size.times do
              ws = new_ws(url, **cbs)

              ws.wait_opened { |ws| pool.push(ws) }
            end
          end
        end

        # 关闭连接池
        def close_pool
          pool.each { |ws| ws.close! }
          pool.clear
        end

        def [](idx) = pool[idx]

        def push(ws) = pool.push(ws)

        def shift = pool.shift

        # 给定语句块时，语句块将作为获取到ws后的回调，获取到的ws作为语句块参数
        # 不给语句块时，将阻塞直到获取到ws
        # 使用阻塞方式时，要注意不要和异步初始化放在一起，它会让异步操作进入不了下一个tick loop
        def shift!
          if block_given?
            EM.schedule do
              timer = EM::PeriodicTimer.new(0.01) do
                (yield shift; timer.cancel) if any?
              end
            end
            return
          end

          sleep 0.01 while empty?
          shift
        end

        alias get_ws shift!

        def empty? = pool.empty?

        def any? = pool.any?

        def size = pool.size

        def delete_by_uuid(uuid)
          pool.delete_if { |w| w.uuid == uuid }
        end

        def wait_pool_init
          EM.schedule do
            timer = EM::PeriodicTimer.new(0.01) do
              if pool_size == pool.size
                yield pool
                timer.cancel
              end
            end
          end
        end

      end
    end
  end
end
