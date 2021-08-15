require_relative './base'
require_relative './network/rest'
require_relative './network/websocket_tracker'
require_relative './network/websocket_kline'
require 'async'
require 'async/io'

module HuobiApi
  module Network

    # 创建子进程去获取一次性k线数据以及订阅实时数据
    # 返回[rd_req, rd_sub]
    #   - rd_req用于一次性请求数据的管道读
    #   - rd_sub用于实时K线数据的管道读
    # 从这两个io对象读取数据时因使用gets读取，读取的结果是字符串，
    # 因此，需eval STR转换一次得到对应的K线数据(Hash类型)
    # 例如：while (res = rd_req.gets) do kline = eval res end
    # ***** 不能在Async内部调用该方法 *****
    def self.klines_pipe(symbols, types = nil)
      rd_req, wr_req = IO.pipe
      rd_sub, wr_sub = IO.pipe

      if types.nil?
        req_types = %w(1min 5min 15min 30min 60min 1day 1week)
      else
        req_types = Array[*types]
      end

      pid = Process.fork do
        Thread.new { EM.run }

        rd_req.close
        rd_sub.close
        req_kline = HuobiApi::Network::WebSocket::ReqKLine.new
        sub_kline = HuobiApi::Network::WebSocket::SubKLine.new

        Async do |task|
          sub_kline.sub_coins_kline(symbols)
          # 请求指定币的一次性K线数据
          req_types.each do |type|
            req_kline.req_coins_kline(symbols, type, 50)
          end
        end

        sub_queue = sub_kline.queue # 订阅实时K线数据的队列
        req_queue = req_kline.queue # 一次性请求K线数据的队列
        # 获取一次性请求的次数，如果次数等于symbols的数量，
        # 则关闭req_kline的连接池，并关闭管道写端wr_req
        req_fetch_times = 0

        Async do |subtask|
          while true
            while (data = sub_queue&.shift)
              wr_sub.write(data)
              wr_sub.write("\n") # 每次写入追加一个换行符，使得读取时可以gets方法读取
            end

            while (data = req_queue&.shift)
              wr_req.write(data)
              wr_req.write("\n") # 每次写入追加一个换行符，使得读取时可以gets方法读取

              req_fetch_times += 1
              if req_fetch_times == symbols.size * req_types.size
                req_kline.ws_pool.close_pool
                wr_req.close
              end
            end

            subtask.sleep 0.002
          end
        end
      end

      old_trap = trap('EXIT') {
        old_trap&.call
        Process.kill('TERM', pid)
      }

      wr_req.close
      wr_sub.close
      [rd_req, rd_sub]
    end
  end
end







