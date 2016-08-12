require 'kafka-consumer'
require 'dotenv'
require 'octocore'
require 'octorecommender'
require 'daemons'
require 'octonotification'
require 'octomessageadapters'

Dotenv.load

class EventsConsumer
  include Octo::Helpers::ApiConsumerHelper

  ZOOKEEPER = ENV['ZOOKEEPER']

  def initialize(config_file=nil)
    if config_file.nil?
      config_file = File.join(File.expand_path(File.dirname(__FILE__)), 'config')
    end
    Octo.connect_with config_file
    @consumer = Kafka::Consumer.new(Octo.get_config(:kafka).fetch(:client_id, 'apiconsumer' + rand(100).to_s),
                                    Octo.get_config(:kafka).fetch(:topic),
                                    zookeeper: Octo.get_config(:zookeeper, ZOOKEEPER),
                                    logger: Octo.logger)
    Signal.trap('INT') { @consumer.interrupt }
  end

  def startConsuming
    @consumer.each do |message|
      begin
        handle(message.value)
      rescue Exception => e
        Octo.logger.error(e)
      end
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

  opts = {
    app_name: 'api_consumer',
    dir_mode: :script,
    dir: 'shared/pids',
    log_dir: "#{ curr_dir }/shared/log",
    log_output: true,
    monitor: true,
    multiple: true
  }

  config_file = File.join(curr_dir, 'config')

  Daemons.run_proc('api_consumer', opts) do
    main(config_file)
  end
end
