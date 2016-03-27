require 'kafka-consumer'

require 'octocore'
require 'productRecommender'

require_relative 'lib/consume'

class EventsConsumer

  ZOOKEEPER = '127.0.0.1:2181'
  CLIENT_ID = 'eventsConsumer'
  TOPICS    = ['events']

  def initialize
    @consumer = Kafka::Consumer.new(CLIENT_ID,
                                    TOPICS,
                                    zookeeper: ZOOKEEPER,
                                    logger: nil)
    Signal.trap("INT") { @consumer.interrupt }

    Octo.connect_with_config_file(File.join(Dir.pwd, 'config', 'config.yml'))
  end

  def startConsuming
    octoConsumer = Octo::Consumer.new
    @consumer.each do |message|
      octoConsumer.handle(message.value)
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
