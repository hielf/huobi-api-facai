#!/usr/bin/env ruby
#
require "bundler/setup"
require 'gdbm'
require 'oj'
require 'multi_json'

def run(pattern = nil)
  db_dir = '/mnt/v/huobi_klines'
  Dir.chdir db_dir
  Dir.glob("*_#{pattern ? pattern : ''}*")
     .each_slice(8)
     .each_with_index do |some_db, idx|

        p [(idx + 1) * 8, some_db]

        some_db.each do |file|
          Process.fork do
            symbol, type = file.split('_')
            # p [file, Time.now.strftime("%F %T.%3N")]
            GDBM.open(db_dir + '/' + file) do |db|
              yield db, symbol, type
            end
            # p [file, Time.now.strftime("%F %T.%3N"), '--------']
          end
        end
        Process.waitall
     end
end

def run_one(filename)
  db_dir = '/mnt/v/huobi_klines'
  symbol, type = filename.split('_')
  GDBM.open(db_dir + '/' + filename) do |db|
    yield db, symbol, type
  end
end

def distance(type)
  case type
  when '1min' then 60
  when '5min' then 300
  when '15min' then 900
  when '30min' then 1800
  when '60min' then 3600
  when '1day' then 86400
  when '1week' then 7 * 86400
  end
end

# 添加最新epoch
def update_meta_max_epoch(pattern = nil)
  run(pattern) do |db, symbol, type|
     next if db["max_epoch"]
     max_epoch = db.max_by {|x, _| x.to_i}
     db["max_epoch"] = max_epoch[0]
  end
end

def update_meta_keys(pattern = nil)
  run(pattern) do |db, symbol, type|
    next if db['keys']
    keys = db.keys
             .filter_map {|x| x.to_i if x.to_i > 0}

    db['keys'] = MultiJson.dump(keys.uniq)
  end
end

# 验证db文件中的key是否都合理(相邻两个key是否相差900 * distance(type))
def find_invalid_keys(pattern = nil)
  run(pattern) do |db, symbol, type|
    keys = MultiJson.load(db['keys'] || '[]')
    (p [symbol, type, 'keys empty'];next) if keys.empty?

    keys = keys.map(&:to_i).sort

    max_epoch = MultiJson.load(db['max_epoch'] || '0').to_i

    if keys[-1] != max_epoch
      p [symbol, type, {max_epoch: max_epoch, max_key:  keys[-1]}]
      next
    end

    keys.each_cons(2).any? do |x, y|
      (p [symbol, type, x, y, y - x]; true) if y - x != 900 * distance(type)
    end
  end
end

def validate_size(filename)
  run_one(filename) do |db, symbol, type|
    keys = JSON.parse(db['keys']) - [db['max_epoch'].to_i]
    keys.each do |key|
      size = JSON.parse(db[key.to_s]).size
      p [symbol, type, key, size] if size != 900
    end
  end
end


require "irb"
IRB.start(__FILE__)

