# frozen_string_literal: true

require "test_helper"

HuobiApi.configure do |config|
  config.proxy = 'www.xxx.com'
  config.access_key = 'xxx'
  config.secret_key = 'xxx'

  config.log_level = 'debug'
end

class HuobiApiTest < Minitest::Test
  def test_account_id
    assert Integer === HuobiApi::Account::account_id
  end

  def test_coin_balance
    coin_balance = HuobiApi::Account::coin_balance('usdt')
    assert coin_balance.empty? || coin_balance.except(:trade, :fronzen).any?
  end

  def test_coin_precision
    coins = HuobiApi::Coins.new
    assert_equal 2, coins.coin_amount_precision('dkausdt')
  end
end
