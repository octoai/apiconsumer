require 'kafka-consumer'
require 'dotenv'
require 'octocore'
require 'octorecommender'

Dotenv.load

class EventsConsumer
  include Octo::Helpers::ApiConsumerHelper

  ZOOKEEPER = ENV['ZOOKEEPER']

  def initialize
    Octo.connect_with_config_file(File.join(Dir.pwd, 'config', 'config.yml'))
    @consumer = Kafka::Consumer.new(Octo.get_config(:client_id, 'apiconsumer'),
                                    Octo.get_config(:kafka).fetch(:topic),
                                    zookeeper: ZOOKEEPER,
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

def main
  ec = EventsConsumer.new
  ec.startConsuming
end

if __FILE__ == $0
  main
end
