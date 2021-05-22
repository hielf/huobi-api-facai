# frozen_string_literal: true

require_relative "huobi_api/version"
require_relative './huobi_api/network'
require_relative './huobi_api/account'
require_relative './huobi_api/coins'
require_relative './huobi_api/order'


module HuobiApi
  class Error < StandardError; end
  # Your code goes here...
end


if __FILE__ == $PROGRAM_NAME
  require 'pry'
  p HuobiAPI::Account.account_id
  p HuobiAPI::Account.coin_balance('usdt')

  coins = HuobiAPI::Coins.new
  p coins.coin_amount_precision('dkausdt')
  p coins.coin_price_precision('dkausdt')

  order = HuobiAPI::Order.new('dkausdt')
  p order.order_history
  # p order.submit_cancel(280980534854879)
  # p order.submit_cancel_all
  # p order.open_orders
  # p order.order_details(280980534854879)

  binding.pry
end
