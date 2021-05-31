require_relative './network'

module HuobiApi
  module Account
    @account_id = nil

    # @return [Integer]
    def self.account_id
      return @account_id if @account_id

      res = HuobiApi::Network::Rest.send_req('get', '/v1/account/accounts')
      @account_id = res['data']&.each { |info| break info['id'] if info['type'] == 'spot' }
      @account_id
    end

    # @return [Hash{String->String}]
    def self.balance
      res = HuobiApi::Network::Rest.send_req('get', "/v1/account/accounts/#{account_id}/balance")
      res
    end

    # @param [String] coin
    # @return [Hash{String->Float}] or {}
    def self.coin_balance(coin)
      balance_list = balance.dig('data','list')
      coin = coin.downcase
      balances = balance_list.filter { |info| info['currency'] == coin }

      balances.each_with_object({}) do |info, h|
        h[info['type']] = info['balance'].to_f
      end
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  puts HuobiApi::Account.account_id
  puts HuobiApi::Account.coin_balance('usdt')
  puts HuobiApi::Account.coin_balance('dkausdt')
end



