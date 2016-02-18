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
    mapEventsToStatements
  end

  # Prepare all possible insert statements
  def prepareStatements
    @appInitStatement = @session.prepare(
      "INSERT INTO app_init JSON ?")
    @appLoginStatement = @session.prepare(
      "INSERT INTO app_login JSON ?")
    @appLogoutStatement = @session.prepare(
      "INSERT INTO app_logout JSON ?")
    @pageViewStatement = @session.prepare(
      "INSERT INTO page_view JSON ?")
    @productPageViewStatement = @session.prepare(
      "INSERT INTO productpage_view JSON ?")
  end

  # Maps the event names to prepared statements for execution
  def mapEventsToStatements
    @eventsMap = {
      'app.init' => @appInitStatement,
      'app.login' => @appLoginStatement,
      'app.logout' => @appLogoutStatement,
      'page.view' => @pageViewStatement,
      'productpage.view' => @productPageViewStatement
    }
  end

  # Write message to cassandra
  # @param [Hash] msg The message hash
  def write(msg)
    msgParsed = parse(msg)
    stmt = @eventsMap.fetch(msgParsed.delete(:event_name))
    @session.execute(stmt, arguments: [JSON.generate(msgParsed)])
  end

  # Parse the message
  # @param [Hash] msg The message hash
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
    case msg['event_name']
    when 'page.view'
      m.merge!({
        routeUrl: msg['routeUrl'],
        categories: msg['categories'],
        tags: msg['tags']
      })
    when 'productpage.view'
      m.merge!({
        routeUrl: msg['routeUrl'],
        categories: msg['categories'],
        tags: msg['tags'],
        productId: msg['productId'],
        productName: msg['productName'],
        price: msg['price']
      })
    end
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
