require 'logger'

module HuobiApi
  module Log
    class << self
      attr_reader :log

      def method_missing(name, *args, &block)
        if %i(debug info warn error fatal).include? name
          self.new unless log
          log.send(name, *args, &block)
        else
          super(name, *args, &block)
        end

      end
    end

    def self.new
      return @log if @log

      log = Logger.new(HuobiApi.log_file || STDOUT)
      log.formatter = proc do |severity, datetime, progname, msg|
        "[#{datetime.strftime('%Y-%m-%d %H:%M:%S.%3N')} #{severity}] #{progname}: #{msg}\n"
      end
      log.level = HuobiApi.log_level || Logger::INFO
      @log = log
      log
    end

    # def self.debug(...)
    #   self.new unless log
    #   log.debug(...)
    # end
    #
    # def self.info(...)
    #   self.new unless log
    #   log.info(...)
    # end
    #
    # def self.warn(...)
    #   self.new unless log
    #   log.warn(...)
    # end
    #
    # def self.error(...)
    #   self.new unless log
    #   log.error(...)
    # end
    #
    # def self.fatal(...)
    #   self.new unless log
    #   log.fatal(...)
    # end
  end
end


