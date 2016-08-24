require 'kafka-consumer'
require 'redis'
require 'redis-queue'
require 'dotenv'
require 'octocore'
require 'octorecommender'
require 'daemons'
require 'octonotification'

Dotenv.load

class EventsConsumer
  include Octo::Helpers::ApiConsumerHelper

  ZOOKEEPER = ENV['ZOOKEEPER']

  def initialize(config_file=nil)
    
    if config_file.nil?
      config_file = File.join(File.expand_path(File.dirname(__FILE__)), 'config')
    end
    Octo.connect_with config_file

    # Check if kafka enabled
    if Octo.get_config(:kafka).fetch(:enabled)

      @consumer = Kafka::Consumer.new(Octo.get_config(:kafka).fetch(:client_id, 'apiconsumer' + rand(100).to_s),
                                    Octo.get_config(:kafka).fetch(:topic),
                                    zookeeper: Octo.get_config(:zookeeper, ZOOKEEPER),
                                    logger: Octo.logger)
      Signal.trap('INT') { @consumer.interrupt }
    else # Using Redis

      default_config = {
        host: '127.0.0.1', port: 6379
      }
      redis = Redis.new(Octo.get_config(:redis, default_config))
      @queue = Redis::Queue.new('octo', 'message', :redis => redis)
    end
  end

  def startConsuming
    begin
      if Octo.get_config(:kafka).fetch(:enabled)
        @consumer.each do |message|
          handle(message.value)
        end
      else # Using Redis
        while message=@queue.pop
          handle(message)
        end
      end
    rescue Exception => e
      puts e
      Octo.logger.error(e)
    end
  end

end

def main(config_file)
  ec = EventsConsumer.new(config_file)
  ec.startConsuming
end

if __FILE__ == $0

  STDOUT.sync = true

  curr_dir = File.expand_path(File.dirname(__FILE__))
  config_file = File.join(curr_dir, 'config')

  daemon_mode = !('true' == ENV['FOREGROUND'])
  puts daemon_mode
  if daemon_mode
    opts = {
      app_name: 'api_consumer',
      dir_mode: :script,
      dir: 'shared/pids',
      log_dir: "#{ curr_dir }/shared/log",
      log_output: true,
      monitor: true,
      multiple: true
    }
    Daemons.run_proc('api_consumer', opts) do
      main(config_file)
    end
  else
    main config_file
  end
end

