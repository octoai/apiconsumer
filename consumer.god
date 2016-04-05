
curr_dir = File.expand_path(File.dirname(__FILE__))
script_path = File.join(curr_dir, 'consumer.rb')

God.watch do |w|
  w.name = "eventsconsumer"
  w.start = "ruby #{ script_path }"
  w.keepalive(:memory_max => 150.megabytes,
              :cpu_max => 50.percent)
end