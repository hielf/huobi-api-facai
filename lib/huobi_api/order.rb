require_relative './network'
require_relative './coins'
require_relative './account'

module HuobiApi
  class Order
    attr_reader :symbol, :price_precision, :amount_precision

    def initialize(symbol)
      coins = HuobiApi::Coins.new

      symbol = symbol.end_with?('usdt') ? symbol : "#{symbol}usdt"
      unless coins.valid_symbol?(symbol)
        raise "invalid symbol: <#{symbol}>"
      end

      @symbol = symbol
      @price_precision = coins.coin_price_precision(symbol)
      @amount_precision = coins.coin_amount_precision(symbol)
    end

    # 下单(此方法不提供策略下单功能(algo-order))
    # @param amount
    # @param order_type: limit(限价单)，market(市价单)，stop-limit(止盈止损单)，ioc，limit-fok，stop-limit-fok
    # @param options: {price: xxx}或{price: xxx, "stop-price":xxx, operator: 'gte' | 'lte'}或空{}
    def order_place(amount, order_type, **options)
      unless Integer === amount or Float === amount
        Log.error("#{self.class}#order_place") { "argument wrong 1" }
        return
      end

      valid_order_types = %w[buy-ioc            sell-ioc
                             buy-limit          sell-limit
                             buy-market         sell-market
                             buy-limit-fok      sell-limit-fok
                             buy-stop-limit     sell-stop-limit
                             buy-limit-maker    sell-limit-maker
                             buy-stop-limit-fok sell-stop-limit-fok
                            ]
      unless valid_order_types.include? order_type
        Log.error("#{self.class}#order_place") { "argument wrong 2" }
        return
      end

      valid_options_keys = %i[price source client-order-id stop-price operator]
      if options.transform_keys!(&:to_sym).except(*valid_options_keys).any?
        Log.error("#{self.class}#order_place") { "argument wrong 3" }
        return
      end

      path = '/v1/order/orders/place'
      req_data = {
        amount: amount,
        symbol: @symbol,
        type: order_type,
        'account-id': HuobiApi::Account.account_id,
        source: 'api',
      }.merge(options)

      res = HuobiApi::Network::Rest.send_req('post', path, req_data)
      res
    end
    private :order_place

    # 限价买、卖单
    # @param order_type: 'buy' or 'sell'
    # @param price: 限价挂单价格
    # @param amount: 指定挂单数量，{usdt_amount: xxx}表示usdt数量, {coin_amount: xxx}表示币数量
    def limit_order(order_type, price, **amount)
      if amount.size != 1 or amount.transform_keys!(&:to_sym)
                                   .except(:usdt_amount, :coin_amount)
                                   .any?
        Log.error("#{self.class}#limit_order") { "argument wrong" }
        return
      end

      case [price, order_type]
      in [Float | Integer, 'sell' | 'buy']
      else
        Log.error("#{self.class}#limit_order") { "argument wrong" }
        return
      end

      price = price.to_f.truncate(@price_precision)
      order_amount = amount[:usdt_amount] ? (amount[:usdt_amount] / price) : amount[:coin_amount]
      order_amount = order_amount.to_f.truncate(@amount_precision)

      order_place(order_amount, "#{order_type}-limit", price: price)
    end

    # 止盈、止损单
    # {"stop-price"=>"0.38",
    #  "operator"=>"gte",
    #  "filled-amount"=>"0.0",
    #  "filled-fees"=>"0.0",
    #  "symbol"=>"aeusdt",
    #  "source"=>"spot-android",
    #  "client-order-id"=>"",
    #  "amount"=>"89.780000000000000000",
    #  "account-id"=>19452218,
    #  "price"=>"0.390000000000000000",
    #  "created-at"=>1621599201022,
    #  "filled-cash-amount"=>"0.0",
    #  "id"=>282254729329850,
    #  "state"=>"created",
    #  "type"=>"sell-stop-limit"}
    # @param order_type: 'buy' or 'sell'
    # @param price: 挂单价格
    # @param stop_price: 触发价格
    # @param operator: 'gte' or 'lte'，即大于等于触发价时或小于等于触发价时，使用挂单价格挂单
    # @param amount: {usdt_amount: xxx}时表示买卖多少数量的USDT，或{coin_amount: xxx}表示买卖多少数量的币
    def stop_limit_order(order_type, price, stop_price, operator, **amount)
      # order_type参数判断
      if order_type != 'buy' and order_type != 'sell'
        Log.error("#{self.class}#stop_limit_order") { "argument wrong" }
        return
      end

      # price、stop_price、operator参数判断
      case [price, stop_price, operator]
      in [Integer | Float, Integer | Float, 'gte' | 'lte']
      else
        Log.error("#{self.class}#stop_limit_order") { "argument wrong" }
        return
      end

      # amount参数判断
      if amount.size != 1 or amount.transform_keys!(&:to_sym)
                                   .except(:usdt_amount, :coin_amount)
                                   .any?
        Log.error("#{self.class}#limit_order") { "argument wrong" }
        return
      end

      price = price.to_f.truncate(@price_precision)
      order_amount = amount[:usdt_amount] ? (amount[:usdt_amount] / price) : amount[:coin_amount]
      order_amount = order_amount.to_f.truncate(@amount_precision)

      order_place(order_amount, "#{order_type}-stop-limit", **{
        price: price,
        'stop-price': stop_price,
        operator: operator
      })
    end

    # 市价买、卖单
    # @param order_type: 'buy' or 'sell'
    # @param amount: 市价买单时，为usdt数量表示买入多少usdt，市价卖单时，为币的数量
    def market_order(order_type, amount)
      if order_type != 'buy' and order_type != 'sell'
        Log.error("#{self.class}#market_order") { "argument wrong" }
        return
      end

      # 市价卖出时，修正卖出币数量的精度
      amount = order_type == 'buy' ? amount : amount.to_f.truncate(@amount_precision)
      order_place(amount, "#{order_type}-market")
    end

    # openOrder: 查询已提交但是仍未完全成交或未被撤销的订单
    def open_orders
      path = '/v1/order/openOrders'
      req_data = { symbol: @symbol, 'account-id': HuobiApi::Account.account_id }
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      res['status'] == 'ok' ? res['data'] : nil
    end

    # 查询历史订单信息
    def order_history
      path = '/v1/order/orders'
      states = 'created,submitted,partial-filled,filled,canceling,canceled,partial-canceled'
      req_data = {
        states: states,
        symbol: @symbol,
        'account-id': HuobiApi::Account.account_id
      }
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      res['status'] == 'ok' ? res['data'] : nil
    end

    # 获取订单详情
    def order_details(order_id)
      path = "/v1/order/orders/#{order_id}"
      res = HuobiApi::Network::Rest.send_req('get', path)
      res['status'] == 'ok' ? res['data'] : nil
    end

    # 撤单
    # 撤单成功：{"status"=>"ok", "data"=>"<order_id>"}
    def submit_cancel(order_id)
      path = "/v1/order/orders/#{order_id}/submitcancel"
      res = HuobiApi::Network::Rest.send_req('post', path)
      res
    end

    # 撤单该币所有订单
    def submit_cancel_all
      path = '/v1/order/orders/batchCancelOpenOrders'
      req_data = {
        'account-id': HuobiApi::Account.account_id,
        symbol: self.symbol, # 如果symbol字段为'all'，将撤销所有币的挂单
      }
      res = HuobiApi::Network::Rest.send_req('post', path, req_data)
      res
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  require 'pry'
  binding.pry
  _order = HuobiApi::Order.new('dkausdt')
  # p order.submit_cancel(280980534854879)
  # p order.submit_cancel_all
  # p order.open_orders
  # p order.order_history
  # p order.order_details(280980534854879)
end




