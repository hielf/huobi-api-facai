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

    # @return [Hash{String->String}, nil]
    def self.balance
      res = HuobiApi::Network::Rest.send_req('get', "/v1/account/accounts/#{account_id}/balance")
      res["status"] == "ok" ? res.dig('data', 'list') : nil
    end

    # @param [String] coin
    # @return [Hash{String->Float},{}]
    def self.coin_balance(coin)
      coin = coin.downcase.delete_suffix('usdt') if /usdt$/i.match?(coin)

      balance_list = balance
      if balance_list.nil?
        {}
      else
        balances = balance_list.select { |info| info['currency'] == coin.downcase }
        balances.each_with_object({}) do |info, h|
          h[info['type']] = info['balance'].to_f
        end
      end

    end
  end
end



