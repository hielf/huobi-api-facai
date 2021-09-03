require_relative './network'
require_relative './coins'
require_relative './account'
require 'securerandom'

module HuobiApi
  class Order
    attr_reader :symbol, :price_precision, :amount_precision

    # 查询交易费率
    # 成功则返回:
    # {
    #   "code"=>200, "data"=>[
    #     {
    #       "symbol"=>"btcusdt",
    #       "actualMakerRate"=>"0.002",
    #       "actualTakerRate"=>"0.002",
    #       "takerFeeRate"=>"0.002",
    #       "makerFeeRate"=>"0.002"
    #     }
    #   ],
    #   "success"=>true
    # }
    def self.fee_rate
      return @fee_rate if @fee_rate

      path = '/v2/reference/transact-fee-rate'
      req_data = {symbols: 'btcusdt'}
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      @fee_rate = res['success'] ? res['data'][0]['makerFeeRate'].to_f : nil
      @fee_rate
    end

    def initialize(symbol)
      symbol = symbol.end_with?('usdt') ? symbol : "#{symbol}usdt"
      unless HuobiApi::Coins.valid_symbol?(symbol)
        raise "invalid symbol: <#{symbol}>"
      end

      @symbol = symbol
      @price_precision = HuobiApi::Coins.coin_price_precision(symbol)
      @amount_precision = HuobiApi::Coins.coin_amount_precision(symbol)
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
    # 返回：挂单成功时，{data:234748720841723,code:200,success:true}
    def limit_order(order_type, price, **amount)
      if amount.size != 1 or amount.transform_keys!(&:to_sym)
                                   .except(:usdt_amount, :coin_amount)
                                   .any?
        Log.error("#{self.class}#limit_order") { "argument wrong 1" }
        return
      end

      case [price, order_type]
      in [Float | Integer, 'sell' | 'buy']
      else
        Log.error("#{self.class}#limit_order") { "argument wrong 2" }
        return
      end

      price = price.to_f.truncate(@price_precision)
      order_amount = amount[:usdt_amount] ? (amount[:usdt_amount] / price) : amount[:coin_amount]
      order_amount = order_amount.to_f.truncate(@amount_precision)

      # 如果要交易的数量太少，直接返回
      return if order_amount == 0

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
    # 返回：
    #   - 挂单成功：{data:234748720841723,code:200,success:true}
    #   - 挂单失败，则data字段为nil
    def market_order(order_type, amount)
      if order_type != 'buy' and order_type != 'sell'
        Log.error("#{self.class}#market_order") { "argument wrong" }
        return
      end

      # 市价卖出时，修正卖出币数量的精度
      amount = order_type == 'buy' ? amount : amount.to_f.truncate(@amount_precision)
      order_place(amount, "#{order_type}-market")
    end

    # 策略委托：计划委托和追踪委托
    # type: side-type或type-side，side取值：buy,sell，type取值：limit,market，例如limit-buy和buy-limit都有效
    # stop_price: 触发策略委托的触发价
    # options: 从{amount:, price: rate:}取值
    #   price: (仅限价单有效)挂单价格
    #   amount : 买单表示买入USDT数量，卖单表示卖出币的数量
    #   rate: 回调幅度(范围[0.001,0.05])，设置该属性时，表明这是追踪委托，不设置该属性时，表明这是计划委托
    #         注：追踪委托只能使用市价
    # 成功委托的返回值：{"data":{"clientOrderId":"xxx","errCode":null,"errMessage":null},"code":200,"success":true}
    def algo_order(type, stop_price, **options)
      side = type[/buy|sell/]
      type = type[/limit|market/]
      raise "#{self.class}##{__method__.to_s}: argument wrong" if side.nil? or type.nil?

      req_data = {
        accountId: HuobiApi::Account.account_id,
        symbol: @symbol,
        orderSide: side,
        orderType: type,
        clientOrderId: 'algo_api_' + SecureRandom.uuid,
        stopPrice: stop_price.to_f.truncate(@price_precision),
      }

      options = options.transform_keys!(&:to_sym)
      begin
        if type == 'market' and side == 'buy' # 市价买单
          req_data[:orderValue] = options.fetch(:amount)
        elsif type == 'market' and side == 'sell' # 市价卖单
          req_data[:orderSize] = options.fetch(:amount).to_f.truncate(@amount_precision)
        elsif type == 'limit' and side == 'buy'
          price = options.fetch(:price).to_f.truncate(@price_precision)
          req_data[:orderPrice] = price

          coin_amount = options.fetch(:amount) / price.to_f # usdt_amount to coin_amount
          req_data[:orderSize] = coin_amount.to_f.truncate(@amount_precision)
        elsif type == 'limit' and side == 'sell'
          req_data[:orderPrice] = options.fetch(:price).to_f.truncate(@price_precision)
          req_data[:orderSize] = options.fetch(:amount).to_f.truncate(@amount_precision)
        end
      rescue => e
        raise "#{self.class}##{__method__.to_s}: #{e.message}"
      end

      # 追踪委托?
      # 如果存在rate属性，返回删除的值，如果不存在，返回nil
      if (rate = options.delete(:rate))
        if type == 'limit'
          raise "#{self.class}##{__method__.to_s}: trailingOrder only used with market order"
        end
        if (0.001..0.05).include?(rate)
          req_data[:trailingRate] = rate
        else
          raise "#{self.class}##{__method__.to_s}: rate range: [0.001, 0.05]"
        end
      end

      res = HuobiApi::Network::Rest.send_req('post', '/v2/algo-orders', req_data)
      res
    end

    # 策略委托的撤单：只能撤销未触发时的委托单，已触发的委托单需使用submit_cancel来撤单
    def algo_order_cancel(*client_order_ids)
      path = '/v2/algo-orders/cancellation'
      req_data = {
        clientOrderIds: client_order_ids
      }
      res = HuobiApi::Network::Rest.send_req('post', path, req_data)
      res
    end

    # 查询未触发的策略委托单：只能查询未触发时的委托单，已触发的委托单需使用open_orders来查询
    def algo_order_opening(symbol = @symbol)
      path = '/v2/algo-orders/opening'
      req_data = {
        symbol: symbol
      }
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      res
    end

    # 查询策略委托的委托详情
    # 成功时返回
    # {"code"=>200,
    #  "data"=>
    #   {"orderOrigTime"=>1623193741069,
    #    "lastActTime"=>1623193741353,
    #    "symbol"=>"dogeusdt",
    #    "source"=>"api",
    #    "clientOrderId"=>"algo_api_84413ec3-843e-4f74-96c8-eda64c750cda",
    #    "orderSide"=>"sell",
    #    "orderType"=>"market",
    #    "orderSize"=>"18.1",
    #    "accountId"=>19452218,
    #    "timeInForce"=>"ioc",
    #    "stopPrice"=>"0.335228",
    #    "orderStatus"=>"created"}}
    def algo_order_details(client_order_id)
      path = '/v2/algo-orders/specific'
      req_data = {
        clientOrderId: client_order_id,
      }
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      res
    end

    # 查询该币或指定币的策略委托历史
    # order_status:
    #   - canceled表示已撤销的策略委托
    #   - rejected表示委托失败
    #   - triggered表示已触发
    def algo_order_history(symbol = @symbol, order_status)
      path = '/v2/algo-orders/history'
      req_data = {
        symbol: symbol,
        orderStatus: order_status, #'canceled,rejected,triggered',
      }
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      res
    end

    # openOrder: 查询该币或指定币的已提交但仍未完全成交或未被撤销的订单
    def open_orders(symbol = @symbol)
      path = '/v1/order/openOrders'
      req_data = { symbol: symbol, 'account-id': HuobiApi::Account.account_id }
      res = HuobiApi::Network::Rest.send_req('get', path, req_data)
      res['status'] == 'ok' ? res['data'] : nil
    end

    # 查询该币或指定币的历史订单信息
    # 返回：成功后响应的数据格式：
    # {
    #   "status": "ok",
    #   "data": [
    #     {
    #       "id": 235204744123517,
    #       "symbol": "waxpusdt",
    #       "account-id": 19452218,
    #       "client-order-id": "",
    #       "amount": "20.380000000000000000",  // 市价买入时表示委托买入的usdt数量，限价买卖或市价卖出时，表示入时委托的币数量
    #       "price": "0.0",  // 市价买卖时，该字段为0，限价买卖时，该字段大于0
    #       "created-at": 1616149829417,
    #       "type": "sell-market",    // buy-limit(限价买), sell-limit(限价卖), sell-market(市价卖), buy-market(市价买)
    #       "field-amount": "20.380000000000000000", // 表示实际成交的币数量(a)
    #       "field-cash-amount": "4.983195320000000000", // 表示实际成交的usdt数量(b)，b/a就是市价买卖时的成交均价
    #       "field-fees": "0.009966390640000000",
    #       "finished-at": 1616149829431,
    #       "source": "spot-web",
    #       "state": "filled",    //filled表示已完成，canceled表示已撤单
    #       "canceled-at": 0      // 如果是撤单操作，该字段大于0
    #      },
    #      ...
    #   ]
    # }
    def order_history(symbol = @symbol)
      path = '/v1/order/orders'
      states = 'created,submitted,partial-filled,filled,canceling,canceled,partial-canceled'
      req_data = {
        states: states,
        symbol: symbol,
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
      res['status'] == 'ok' ? res['data'] : nil
    end

    # 撤单：基于client_order_id进行撤单
    def cancel_by_client_order_id(client_order_id)
      path = '/v1/order/orders/submitCancelClientOrder'
      req_data = {
        'client-order-id': client_order_id
      }
      res = HuobiApi::Network::Rest.send_req('post', path, req_data)
      res
    end

    # 批量撤单：基于指定的order_id来批量撤单

    # 批量撤单：基于指定的币来撤单该币的所有订单
    def submit_cancel_all(symbol = @symbol)
      path = '/v1/order/orders/batchCancelOpenOrders'
      req_data = {
        'account-id': HuobiApi::Account.account_id,
        symbol: symbol, # 如果symbol字段为'all'，将撤销所有币的挂单
      }
      res = HuobiApi::Network::Rest.send_req('post', path, req_data)
      res
    end
  end
end




