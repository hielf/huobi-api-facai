# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

task(:commit) do 
  sh %q(ruby -i -pe 'gsub(/^(\s+VERSION.*\.)(\d+)"$/){$1 + $2.succ + %Q(")}' lib/huobi_api/version.rb)
  sh %Q(git add .;git commit -m #{Time.now.strftime("%FT%T")};git push origin dev)
end 


task default: :test
