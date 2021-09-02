require_relative './base'
require_relative './network/rest'
require_relative './network/websocket_tracker'
require_relative './network/websocket_kline'
require 'oj'
require 'json'
require 'multi_json'
require 'async'
require 'async/io'
require 'socket'

module HuobiApi
  module Network

    # 创建子进程去获取一次性k线数据以及订阅实时数据，并写入套接字
    # 返回[req_reader, sub_reader]
    #   - req_reader用于一次性请求数据的套接字读端
    #   - sub_reader用于实时K线数据的套接字读端
    # ***** 不能在Async内部调用该方法 *****
    def self.klines_server(symbols, types = nil)
      req_sock_path = '/tmp/req.sock'
      sub_sock_path = '/tmp/sub.sock'

      if types.nil?
        req_types = %w(1min 5min 15min 30min 60min 1day 1week)
      else
        req_types = Array[*types]
      end

      pid = Process.fork do
        req_sock = UNIXServer.new(req_sock_path)
        sub_sock = UNIXServer.new(sub_sock_path)
        req_s = req_sock.accept
        sub_s = sub_sock.accept

        Thread.new { EM.run }

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
        # 则关闭req_kline的连接池
        req_fetch_times = 0

        Async do |subtask|
          while true
            while (data = sub_queue&.shift)
              sub_s.puts(MultiJson.dump(data))
              subtask.sleep 0.001
            end
            Thread.pass

            while (data = req_queue&.shift)
              req_s.puts(MultiJson.dump(data))

              req_fetch_times += 1
              if req_fetch_times == symbols.size * req_types.size
                req_kline.ws_pool.close_pool
                req_sock.shutdown
              end
            end

            subtask.sleep 0.01
          end
        end
      end

      old_trap = trap('EXIT') {
        old_trap&.call
        Process.kill('TERM', pid)
        File.unlink sub_sock_path
        File.unlink req_sock_path
      }

      # 等待子进程先创建好Unix Socket和相关的sock文件，
      # 否则下面创建Unix Socket客户端报错文件不存在
      sleep 0.1
      [UNIXSocket.new(req_sock_path), UNIXSocket.new(sub_sock_path)]
    end
  end
end







