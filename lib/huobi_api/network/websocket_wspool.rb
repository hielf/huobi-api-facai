require_relative './websocket_base'

module HuobiApi
  module Network
    module WebSocket
      class WSPool
        attr_reader :url, :pool_size, :pool, :cbs

        def initialize(pool_size = 32, url, **cbs)
          @url = url
          @pool_size = pool_size
          @pool = []
          @cbs = cbs

          # init_ws_pool(pool_size, url, **cbs)
        end

        def new_ws(url, **cbs)
          begin
            cbs.fetch_values(:on_open, :on_close, :on_error, :on_message)
          rescue KeyError
            raise "argument cbs only supported: on_open, on_close, on_error, on_message: #{cbs.keys}"
          end

          ws = WebSocket::new_ws(url)
          ws.on(:open) { |event| cbs[:on_open].call(event) }
          ws.on(:close) { |event| cbs[:on_close].call(event) }
          ws.on(:error) { |event| cbs[:on_error].call(event) }
          ws.on(:message) { |event| cbs[:on_message].call(event) }
          ws
        end

        # 初始化WS连接池
        # 池中的ws连接均已经处于open状态
        def init_ws_pool(pool_size = @pool_size, url = nil, **cbs)
          Async(annotation: 'init ws pool') do |task|
            pool_size.times do
              ws = new_ws(url || @url, **(cbs.any? || @cbs))
              ws.wait_opened do
                pool.push(ws)
              end
            end
          end

          self
        end

        alias init init_ws_pool

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
        def shift!
          t = Async(annotation: 'shift!') do |task|
            n = 0
            while empty?
              task.sleep 0.05
              n += 1
              (p "ws pool is empty") if n % 500 == 0
            end

            next yield shift if block_given?
            shift
          end

          return t if block_given?
          t.wait
        end

        def empty? = pool.empty?

        def any?(...)
          pool.any?(...)
        end

        def size = pool.size

        def delete_by_uuid(uuid)
          pool.delete_if { |w| w.uuid == uuid }
        end

        # 可给定语句块，此时将异步等待并等待初始化完成后执行语句块，连接池@pool作为语句块参数
        # 如果不给定语句块，则阻塞等待
        def wait_pool_init
          t = Async(annotation: 'wait ws pool init') do |subtask|
            subtask.sleep 0.05 until pool_size == pool.size

            next yield self if block_given?
            self
          end

          return t if block_given?
          t.wait
        end
      end
    end
  end
end
