require 'uri'
require_relative './log'

module HuobiApi
  class << self
    attr_accessor :proxy, :access_key, :secret_key, :log_file, :log_level
    attr_reader :proxy_addr, :proxy_port
  end

  def self.proxy=(url)
    url = 'http://' + url unless url.start_with?('http://', 'https://', 'socks5://')
    @proxy = url

    uri = URI(url)
    @proxy_addr = uri.host
    @proxy_port = uri.port
  end

  def self.log_level=(level)
    @log_level = case level.downcase
                 when 'debug'
                   Logger::DEBUG
                 when 'info'
                   Logger::INFO
                 when 'warn'
                   Logger::WARN
                 when 'error'
                   Logger::ERROR
                 when 'fatal'
                   Logger::FATAL
                 else
                   Logger::UNKNOWN
                 end
  end

  # HuobiApi.configure do |config|
  #   config.proxy = 'https://www.xxx.cn'
  #   config.log_file = 'a.log'
  # end
  def self.configure
    yield self
  end
end

