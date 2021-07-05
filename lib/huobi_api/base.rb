require 'uri'
require_relative './log'

module HuobiApi
  class << self
    attr_accessor :proxy, :access_key, :secret_key, :log_file, :log_level
    attr_reader :proxy_addr, :proxy_port

    def access_key
      @access_key ||= ENV['HUOBI_ACCESS_KEY']
      raise 'ACCESS KEY is required, set the environment variable "HUOBI_ACCESS_KEY"' if @access_key.nil?

      @access_key
    end

    def secret_key
      @secret_key ||= ENV['HUOBI_SECRET_KEY']
      raise 'SECRET KEY is required, set the environment variable "HUOBI_SECRET_KEY"' if @secret_key.nil?

      @secret_key
    end

    def proxy
      @proxy || ENV['http_proxy'] || ENV['HTTP_PROXY']
    end

    def proxy_addr
      @proxy_addr = URI(proxy)&.host
    end

    def proxy_port
      @proxy_port = URI(proxy)&.port
    end
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

