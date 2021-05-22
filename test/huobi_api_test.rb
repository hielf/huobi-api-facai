# frozen_string_literal: true

require "test_helper"

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
