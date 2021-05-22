require_relative './network'

module HuobiAPI
  module Account
    @account_id = nil

    # @return [Integer]
    def self.account_id
      return @account_id if @account_id

      res = HuobiAPI::Network::Rest.send_req('get', '/v1/account/accounts')
      account_id = res['data'][0]['id']
      @account_id = account_id
      account_id
    end

    # @return [Hash{String->String}]
    def self.balance
      res = HuobiAPI::Network::Rest.send_req('get', "/v1/account/accounts/#{account_id}/balance")
      res
    end

    # @param [String] coin
    # @return [Hash{String->Float}]
    def self.coin_balance(coin)
      balance_list = balance['data']['list']
      coin.downcase!
      balances = balance_list.filter { |info| info['currency'] == coin }

      balances.each_with_object({}) do |info, h|
        h[info['type']] = info['balance'].to_f
      end
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  puts HuobiAPI::Account.account_id
  puts HuobiAPI::Account.coin_balance('usdt')
end



