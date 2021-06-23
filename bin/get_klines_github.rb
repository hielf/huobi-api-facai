#!/usr/bin/env ruby

require "bundler/setup"
require 'huobi_api'
require 'async'
require 'gdbm'
require 'json'

HuobiApi.configure do |config|
  config.proxy = 'xxx'
  config.access_key = 'xxx'
  config.secret_key = 'xxx'

  # config.log_file = STDOUT
  # config.log_file = 'a.log'
  config.log_level = 'debug'

  ############## init...
  HuobiApi::Account.account_id
  HuobiApi::Coins.all_coins_info
end

class KlinesDB
  class DB
    attr_reader :db

    # 每个币的每种类型一个dbm文件，例如btcusdt_1min一个db文件，btcusdt_5min一个db文件
    # 每次请求获得的(最多)每900根K线作为一个dbm Key/Value，其key为这最多900根K线的起始epoch
    # @param mode 打开GDBM的标记，当值为r时表示只读打开，其他任何值都表示默认的写打开
    def initialize(symbol, type, mode = nil)
      @symbol = symbol
      @type = type
      @cnt = 900 # 一次尽量且最多存放900根K线

      db_dir = "/mnt/g/huobi_klines"
      File.directory?(db_dir) || Dir.mkdir(db_dir)
      @file_name = "#{symbol}_#{type}"

      mode = (mode == 'r' ? GDBM::READER : GDBM::WRCREAT)
      @db = GDBM.new("#{db_dir}/#{@file_name}", 0666, mode)
    end

    def self.open(symbol, type, mode = 'r')
      db = new(symbol, type)
      return db unless block_given?

      begin
        yield db
      ensure
        db.close
      end
    end

    def save(epoch, klines)
      db[epoch.to_s] = JSON.dump(klines)
    end

    def keys = db.keys

    def [](idx)
      db[idx]
    end

    def []=(k, v)
      db[k] = v
    end

    def get(epoch)
      return nil unless Numeric === epoch

      hash = db.find do |k, _|
        epoch >= k.to_i and epoch < (k.to_i + @cnt * distance(@type))
      end

      return nil if hash.nil?

      JSON.parse hash[1], symbolize_names: true
    end

    def self.get(symbol, type, epoch)
      db = new(symbol, type)
      data = db.get(epoch)
      db.close

      return data unless block_given?
      yield data
    end

    def get_all
      h = {}
      db.keys.sort_by(&:to_i).each do |key|
        h[key.to_i] = JSON.parse(db[key], symbolize_names: true)
      end
      h
    end

    def self.get_all(symbol, type)
      db = new(symbol, type)
      data = db.get_all
      db.close

      return data unless block_given?
      yield data
    end

    def close
      db.close
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

  attr_reader :kline, :klines, :dispatche_thread

  def initialize(pool_size = 90)
    @kline = HuobiApi::Network::WebSocket::ReqKLine.new(pool_size)

    # 每个币的每种类型一个dbm文件，例如btcusdt_1min一个db文件，btcusdt_5min一个db文件
    # 每次请求获得的(最多)每900根K线作为一个dbm Key/Value，其key为这最多900根K线的起始epoch
    # @klines = {
    #   'btcusdt_1min': { epoch1: [klines], epoch2: [klines]},
    #   'btcusdt_5min': { epoch1: [klines], epoch2: [klines]},
    #   'ethusdt_1min': { epoch1: [klines], epoch2: [klines]}
    # }
    @klines = Hash.new do |hash, key|
      hash[key] = {}
    end

    @dispatche_thread = Thread.new do
      dispatch_klines
    end
  end

  # 获取或更新指定一个币或多个币的一种或多种类型的K线
  # @param symbol_or_arr 单个symbol或者symbol数组
  # @param type 值为nil或%w[1min 5min 15min 30min 60min 1day 1week]之一
  def fetch_klines(symbol_or_arr, type = nil)
    symbols = Array[*symbol_or_arr]
    types = type.nil? ? %w[1min 5min 15min 30min 60min 1day 1week] : [type]

    x = ->(symbol, type) {
      # 已保存的最后一组K线起始时间
      from = nil
      latest = nil
      DB.open(symbol, type, 'r') do |db|
        from = db["max_epoch"].to_i  # 获取目前最大的key
        latest = from.nil? ? nil : JSON.parse(db[from.to_s], symbolize_names: true)[-1][:id]
        p [symbol, type, from, latest, Time.now.strftime("%F %T.%3N")]
      end

      # 如果当前已保存的K线是最新的，则不重新获取
      return if latest != nil and Time.now.to_i < latest + distance(type)

      reqs = kline.gen_coin_all_reqs(symbol, type, from: from)
      size = reqs.size
      kline.req_klines_by_reqs(reqs)

      # 请求获取K线后，等待获取完所有K线数据后，将K线保存起来
      Async do |task|
        key = "#{symbol}_#{type}"

        until klines[key].size == size
          p "#{symbol} size: #{klines[key].size}/#{size}, waiting to save"
          task.sleep 0.5
        end

        DB.open(symbol, type) do |db|
          klines[key].each do |start_epoch, data|
            # 保存数据(epoch和对应K线)
            db.save(start_epoch, data)
            # 保存当前所有已保存的最新key
            db["max_epoch"] = start_epoch.to_s if start_epoch > db["max_epoch"].to_i

            klines[key].delete start_epoch
            puts "fetched: [#{symbol}, #{type}, #{start_epoch}, #{data.size}]"
          end
        end
      end
    }

    symbols.product(types).each_slice(20).each do |some_pairs|
      Async do
        some_pairs.each do |symbol, type|
          p [symbol, type, Time.now.strftime("%F %T.%3N")]
          x.call(symbol, type)
        end
      end
    end

    # symbols.each do |symbol|
    #   types.each do |t|
    #     x.call(symbol, t)
    #   end
    # end
  end

  private def dispatch_klines
    queue = kline.queue

    # 处理一次性请求的K线价格数据
    # {
    #   id: "btcusdt"
    #   rep: "market.btcusdt.kline.1min"
    #   status: "ok"
    #   ts: 1616681203184
    #   data: [
    #     {    // 这是第60分钟前的K线数据
    #       amount: 47.58869897859039
    #       close: 50989.63
    #       count: 1327
    #       high: 51129.91
    #       id: 1616677620  // 该K线的起始时间点(秒级epoch)
    #       low: 50986
    #       open: 51125
    #       vol: 2430238.6246752427
    #     },
    #     ... // 第59分钟前、第58分钟前...1分钟前前以及当前所在分钟的K线数据
    #   ]
    # }
    dispatcher = ->(data) {
      symbol = data[:id]
      period = data[:rep].split(".")[-1]
      klines_epoch = data[:data][0][:id] # 第一根K线的epoch时间点
      klines["#{symbol}_#{period}"][klines_epoch] = data[:data]
    }

    Async(annotation: 'call dispatcher') do |task|
      while true
        dispatcher.call(queue.shift) while queue.any?
        task.sleep 0.5
      end
    end
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












