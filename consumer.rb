require 'kafka-consumer'
require 'cassandra'



class CassandraWriter

  KEYSPACE = 'octo'

  def initialize
    # Connects to localhost by default
    @cluster = Cassandra.cluster
  end


  def write(msg)
    query = parse(msg)
    puts query

  end

  def parse(msg)
    return ''
  end
end



class EventsConsumer

  ZOOKEEPER = '127.0.0.1:2181'
  CLIENT_ID = 'eventsConsumer'
  TOPICS    = ['events']

  def initialize
    @consumer = Kafka::Consumer.new(CLIENT_ID,
                                    TOPICS,
                                    zookeeper: ZOOKEEPER)
    Signal.trap("INT") { @consumer.interrupt }
  end

  def startConsuming
    cw = CassandraWriter.new
    @consumer.each do |message|
      cw.write(message.value)
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
