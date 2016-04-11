curr_dir = File.expand_path(File.dirname(__FILE__))
script_path = File.join(curr_dir, 'consumer.rb')
script_name = 'apiconsumer'

require 'dotenv'
Dotenv.load

God.watch do |w|
  w.name = script_name
  w.log = File.join(curr_dir, 'shared', 'logs', script_name + '.log')
  w.dir = curr_dir
  w.env = ENV
  w.pid_file = File.join(curr_dir, 'shared', 'pids', script_name + '.pid')
  w.start = "ruby #{ script_path }"
  w.keepalive(:memory_max => 150.megabytes,
              :cpu_max => 50.percent)
end
