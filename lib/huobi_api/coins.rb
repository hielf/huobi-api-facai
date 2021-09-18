require_relative './network'

module HuobiApi
  module Coins
    @all_coins_info = nil
    @all_symbols = nil

    class << self
      attr_reader :all_coins_info, :all_symbols, :pre_online_symbols
    end

    # get all coin info
    # {
    #   "base-currency": "btc",
    #   "quote-currency": "usdt",
    #   "price-precision": 2,
    #   "amount-precision": 6,
    #   "symbol-partition": "main",
    #   "symbol": "btcusdt",
    #   "state": "online",
    #   "value-precision": 8,
    #   "min-order-amt": 0.0001,
    #   "max-order-amt": 1000,
    #   "min-order-value": 5,
    #   "limit-order-min-order-amt": 0.0001,
    #   "limit-order-max-order-amt": 1000,
    #   "sell-market-min-order-amt": 0.0001,
    #   "sell-market-max-order-amt": 100,
    #   "buy-market-max-order-value": 1000000,
    #   "leverage-ratio": 5,
    #   "super-margin-leverage-ratio": 3,
    #   "funding-leverage-ratio": 3,
    #   "api-trading": "enabled"
    #  }
    def self.all_coins_info_v1
      res = HuobiApi::Network::Rest.send_req('get', '/v1/common/symbols')
      stable_coins = %w[usdc pax dai]
      all_coins_info = res['data'].each_with_object({}) do |info, hash|
        if info['quote-currency'] == 'usdt' && info['state'] != 'offline' &&
          !/^.*\d[sl]$/.match?(info['base-currency']) && # 跳过杠杆币，如fil3s fil3l
          !stable_coins.include?(info['base-currency']) # 跳过稳定币(usdc/pax/dai)
          hash[info['symbol']] = info
        end
      end

      all_coins_info
    end

    #  成功：
    #  {
    #    "status": "ok",
    #    "data": [
    #    {
    #      "withdraw_risk": "1.5",
    #      "symbol_code": "btcusdt",
    #      "fee_precision": 8,
    #      "trade_price_precision": 2,
    #      "trade_amount_precision": 6,
    #      "trade_total_precision": 8,
    #      "base_currency_display_name": "BTC",
    #      "quote_currency_display_name": "USDT",
    #      "etp_leverage_ratio": null,
    #      "funding_leverage_ratio": "3",
    #      "white_enabled": false,
    #      "trade_enabled": true,
    #      "super_margin_leverage_ratio": "3",
    #      "quote_currency": "usdt",
    #      "base_currency": "btc",
    #      "trade_open_at": 1514779200000,
    #      "country_disabled": false,
    #      "tags": "activities",
    #      "symbol_partition": "main",
    #      "partitions": [
    #        {
    #          "id": 9,
    #          "name": "灰度",
    #          "weight": 95
    #        }
    #      ],
    #      "leverage_ratio": 5,
    #      "weight": 999900925,
    #      "direction": null,
    #      "state": "online",
    #      "display_name": "BTC/USDT"
    #    },
    #    ...
    #  ]
    #  }
    def self.all_coins_info_v2 # 通过API方式，还无法获取potential区的币的信息(缺失)
      res = HuobiApi::Network::Rest.send_req('get', '/v2/beta/common/symbols')
      stable_coins = %w[usdc pax dai]
      all_coins_info = res['data'].each_with_object({}) do |info, hash|
        if /^[^*]+USDT$/.match?(info['display_name']) && info['state'] != 'offline' && !stable_coins.include?(info['base_currency'])
          hash[info['symbol_code']] = info
        end
      end

      all_coins_info
    end

    def self.all_coins_info
      return @all_coins_info if @all_coins_info

      all_coins_info = all_coins_info_v1
      @all_coins_info = all_coins_info
      @all_symbols = all_coins_info.select { |_x, y| y["state"] == "online" }.keys
      @pre_online_symbols = all_coins_info.select { |_x, y| y["state"] == "pre-online" }.keys
      all_coins_info
    end

    def self.coin_info(symbol)
      info = all_coins_info[symbol]

      return info unless block_given?
      yield info
    end

    def self.coin_price_precision(symbol)
      all_coins_info.dig(symbol, 'price-precision') ||
        all_coins_info.dig(symbol, 'trade_price_precision')
    end

    def self.coin_amount_precision(symbol)
      all_coins_info.dig(symbol, 'amount-precision') ||
        all_coins_info.dig(symbol, 'trade_amount_precision')
    end

    def self.valid_symbol?(symbol)
      !!all_coins_info[symbol]
    end
  end
end
