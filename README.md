# HuobiApi

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/huobi_api`. To experiment with that code, run `bin/console` for an interactive prompt.

TODO: Delete this and the text above, and describe your gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'huobi_api'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install huobi_api

## Usage

```ruby
HuobiApi.configure do |config|
  config.proxy = 'http://proxy_address:proxy_port'
  config.access_key = 'xxx'
  config.secret_key = 'yyy'

  config.log_file = STDOUT  # filename or ios, when missing, default STDOUT
  config.log_level = 'debug'  # debug, info(default), warn, error, fatal
end
```

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
