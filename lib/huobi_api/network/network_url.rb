module HuobiApi
  module Network
    urls = %w[
      api.huobipro.com
      api.hadax.com
      api.huobi.pro
      api-aws.huobi.pro
      api.huobi.de.com
    ].freeze

    WS_URLS = urls.map {|url| 'wss://' + url}
    REST_URLS = urls.map {|url| 'https://' + url}
  end
end
