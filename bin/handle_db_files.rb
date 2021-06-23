#!/usr/bin/env ruby
#
require "bundler/setup"
require 'gdbm'
require 'json'

def run(pattern = nil)
  db_dir = '/mnt/g/huobi_klines'
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
  db_dir = '/mnt/g/huobi_klines'
  GDBM.open(db_dir + '/' + filename) do |db|
    yield db
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

    db['keys'] = JSON.dump(keys.uniq)
  end
end

# 验证db文件中的key是否都合理(相邻两个key是否相差900 * distance(type))
def find_invalid_keys(pattern = nil)
  run(pattern) do |db, symbol, type|
    db.keys
      .filter_map {|x| x.to_i if x.to_i > 0}
      .sort
      .each_cons(2)
      .any? {|x, y| (p [x, y, y - x, symbol, type]; true) if y - x != 900 * distance(type)}
  end
end



require "irb"
IRB.start(__FILE__)

