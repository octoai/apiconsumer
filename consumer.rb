require 'kafka-consumer'
require 'cassandra'
require 'json'
require 'set'

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
      "INSERT INTO app_init JSON ?"
    )
    @appLoginStatement = @session.prepare(
      "INSERT INTO app_login JSON ?"
    )
    @appLogoutStatement = @session.prepare(
      "INSERT INTO app_logout JSON ?"
    )
    @pageViewStatement = @session.prepare(
      "INSERT INTO page_view JSON ?"
    )
    @productPageViewStatement = @session.prepare(
      "INSERT INTO productpage_view JSON ?"
    )
    @createUserStatement = @session.prepare(
      "INSERT INTO user_intent (userId, enterpriseId, created_at, updated_at)
      VALUES (?, ?, ?, ?)"
    )
    @selectUserStatement = @session.prepare(
      "SELECT userid, enterpriseid FROM user_intent
      WHERE userid = ? AND enterpriseid = ?"
    )
    @updateUserIntentStatement = @session.prepare(
      "UPDATE user_intent SET updated_at = ?
      WHERE userid = ? AND enterpriseid = ?"
    )
    @selectProductStatement = @session.prepare(
      "SELECT enterpriseid, id, name, price FROM products
      WHERE enterpriseid = ? AND id = ?"
    )
    @createProductStatement = @session.prepare(
      "INSERT INTO products JSON ?"
    )
    @updateProductStatement = @session.prepare(
      "UPDATE products SET price = ?, name = ?
      WHERE enterpriseid = ? AND id = ?"
    )
    @updatePushKey = @session.prepare(
      "INSERT INTO push_keys \
      (enterpriseid, pushtype, pushkey, created_at, updated_at) \
      VALUES (?, ?, ?, ?, ?)"
    )
    @selectPushKey = @session.prepare(
      "SELECT pushkey FROM push_keys WHERE \
      enterpriseid = ? AND pushtype = ?"
    )
    @updateUserPushToken = @session.prepare(
      "INSERT INTO push_tokens JSON ?"
    )
  end

  # Maps the event names to prepared statements for execution
  def mapEventsToStatements
    @eventsMap = {
      'app.init' => [@appInitStatement, @createUserStatement],
      'app.login' => [@appLoginStatement, @createUserStatement],
      'app.logout' => @appLogoutStatement,
      'page.view' => @pageViewStatement,
      'productpage.view' => [@productPageViewStatement, @createProductStatement]
    }
    @eventsCallbacks = {
      'update.push_token' => ['updateEnterprisePushKey', 'updateUserPushToken']
    }
  end

  # Updates the Push Key for the enterprise
  # @param [Hash] msg The message hash to to used
  def updateEnterprisePushKey(msg)
    if msg[:pushKey].nil? or msg[:pushKey].length == 0
      return
    end
    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    args = [eid, msg[:pushType]]
    res = @session.execute(@selectPushKey, arguments: args)
    if res.length == 0
      args = [eid, msg[:pushType], msg[:pushKey], Time.now, Time.now]
      @session.execute(@updatePushKey, arguments: args)
    else
      r = res.first
      if r['pushkey'] != msg[:pushKey]
        args = [eid, msg[:pushType], msg[:pushKey], Time.now, Time.now]
        @session.execute(@updatePushKey, arguments: args)
      end
    end
  end

  # Update the user push token
  def updateUserPushToken(msg)
    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    args = [JSON.generate({
      enterpriseid: eid,
      userid: msg[:userId],
      pushtype: msg[:pushType],
      pushtoken: msg[:pushToken]
    })]
    @session.execute(@updateUserPushToken, arguments: args)
  end

  # Write message to cassandra
  # @param [Hash] msg The message hash
  def write(msg)
    msgParsed = parse(msg)
    eventName = msgParsed.delete(:event_name)


    # Execute the callbacks first
    callbacks = @eventsCallbacks.fetch(eventName, [])
    if callbacks.length > 0
      callbacks.each { |cb|
        mtd = method(cb.to_sym)
        mtd.call(msgParsed)
      }
    end

    # Execute the statements later
    stmt = @eventsMap.fetch(eventName, nil)
    if stmt.class == Array
      stmt.each do |s|
        if s == @createUserStatement
          # check if the user exists already
          args = [msgParsed[:userId].to_i,
                  Cassandra::Uuid.new(msgParsed[:enterpriseId])]
          result = @session.execute(@selectUserStatement, arguments: args)

          if result.size == 1
            args.unshift(Time.now)
            @session.execute(@updateUserIntentStatement, arguments: args)
          elsif result.size == 0
            args.concat([Time.now] * 2)
            @session.execute(s, arguments: args)
          end
        elsif s == @createProductStatement
          # check if the product already exists
          args = [Cassandra::Uuid.new(msgParsed[:enterpriseId]),
                  msgParsed[:productId]]
          result = @session.execute(@selectProductStatement, arguments: args)
          if result.size == 0
            product_msg = {
              id: msgParsed[:productId].to_i,
              enterpriseId: Cassandra::Uuid.new(msgParsed[:enterpriseId]),
              price: msgParsed[:price].to_f,
              name: msgParsed[:productName],
              categories: msgParsed[:categories],
              tags: msgParsed[:tags]
            }
            args = [JSON.generate(product_msg)]
            @session.execute(@createProductStatement, arguments: args)
          elsif result.size == 1
            # if already exists, find if name, price changed, update them
            result.each do |r|
              if (r['name'] != msgParsed[:productName] or \
                  r['price'] != msgParsed[:price])
                args.unshift(msgParsed[:productName].to_s)
                args.unshift(msgParsed[:price].to_f)
                @session.execute(@updateProductStatement, arguments: args)
              end
            end
          end
        else
          args = [JSON.generate(msgParsed)]
          @session.execute(s, arguments: args)
        end
      end
    elsif stmt.class == Cassandra::Statements::Prepared
      begin
        args = [JSON.generate(msgParsed)]
        @session.execute(stmt, arguments: args)
      rescue Exception => e
        puts e
      end
    end
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
        categories: Set(msg['categories']),
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
    when 'update.push_token'
      m.merge!({
        pushType: msg['notificationType'],
        pushKey: msg['pushKey'],
        pushToken: msg['pushToken']
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
                                    zookeeper: ZOOKEEPER,
                                    logger: nil)
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
