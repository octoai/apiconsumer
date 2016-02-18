require 'kafka-consumer'
require 'cassandra'
require 'json'

class CassandraWriter

  KEYSPACE = 'octo'

  def initialize
    # Connects to localhost by default
    @cluster = Cassandra.cluster
    @session = @cluster.connect(KEYSPACE)
    prepareStatements
  end

  def prepareStatements
    @appInitStatement = @session.prepare("INSERT INTO app_init JSON ?")
    @appLoginStatement = @session.prepare("INSERT INTO app_login JSON ?")
    @appLogoutStatement = @session.prepare("INSERT INTO app_logout JSON ?")
  end


  def write(msg)
    msgParsed = parse(msg)
    puts msgParsed[:event_name]
    case msgParsed.delete(:event_name)
    when 'app.init'
      @session.execute(@appInitStatement, arguments: [JSON.generate(msgParsed)])
    when 'app.login'
      @session.execute(@appLoginStatement, arguments: [JSON.generate(msgParsed)])
    when 'app.logout'
      @session.execute(@appLogoutStatement, arguments: [JSON.generate(msgParsed)])
    end

  end

  def parse(msg)
    msg2 = JSON.parse(msg)
    msg = msg2
    m = {
      id: msg['uuid'],
      enterpriseId: msg['enterprise']['id'],
      event_name: msg['event_name'],
      phone: msg.fetch('phoneDetails', {}),
      userId: msg.fetch('userId', -1),
      created_at: Time.now
    }
    return m
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
