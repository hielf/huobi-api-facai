require 'logger'

module HuobiApi
  PROXY_ADDR = 'malongshuai.cn'.freeze
  PROXY_PORT = 8118
  ACCESS_KEY = 'ff974e67-ez2xc4vb6n-b6d764e6-b589d'.freeze
  SECRET_KEY = 'a62b57ce-180b0090-90f1d29a-9a664'.freeze
end


module HuobiApi
  # Log = Logger.new(STDOUT)
  Log = Logger.new('test_log.log')
  Log.formatter = proc do |severity, datetime, progname, msg|
    "[#{datetime.strftime('%Y-%m-%d %H:%H:%S.%3N')} #{severity}] #{progname}: #{msg}\n"
  end
  Log.level = Logger::INFO
end



