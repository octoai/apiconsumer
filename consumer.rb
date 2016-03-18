require 'kafka-consumer'
require 'cassandra'
require 'json'
require 'set'

require_relative 'lib/productRecommender'

class CassandraWriter

  KEYSPACE = 'octo'

  def initialize
    # Connects to localhost by default
    @cluster = Cassandra.cluster
    @session = @cluster.connect(KEYSPACE)
    prepareStatements
    setupObservers
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
    @selectUser = @session.prepare(
      "SELECT userid FROM users \
      WHERE enterpriseid = ? AND userid = ? \
      LIMIT 1"
    )
    @createUser = @session.prepare(
      "INSERT INTO users \
      (enterpriseid, userid, created_at) \
      VALUES (?, ?, ?)"
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
      "INSERT INTO products \
      (enterpriseid, id, categories, name, price, tags) \
      VALUES (?, ?, ?, ?, ?, ?)"
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
    @selectTags = @session.prepare(
      "SELECT id, tag_text FROM tags WHERE \
      enterpriseid = ? AND tag_text IN ?"
    )
    @addTag = @session.prepare(
      "INSERT INTO tags \
      (enterpriseid, tag_text, id, parent, created_at) \
      VALUES (?, ?, ?, ?, ?)"
    )
    @selectCats = @session.prepare(
      "SELECT id, cat_text FROM categories WHERE \
      enterpriseid = ? AND cat_text IN ?"
    )
    @addCat = @session.prepare(
      "INSERT INTO categories \
      (enterpriseid, cat_text, id, parent, created_at) \
      VALUES (?, ?, ?, ?, ?)"
    )
    @selectPage = @session.prepare(
      "SELECT routeurl FROM pages \
      WHERE enterpriseid = ? AND routeurl = ?"
    )
    @createPage = @session.prepare(
      "INSERT INTO pages \
      (enterpriseid, routeurl, categories, created_at, tags) \
      VALUES (?, ?, ?, ?, ?)"
    )
    @updateUserLocation = @session.prepare(
      "INSERT INTO user_location \
      (enterpriseid, userid, created_at, latitude, longitude) \
      VALUES (?, ?, ?, ?, ?)"
    )
    @updateUserPhone = @session.prepare(
      "INSERT INTO user_phone_details \
      (enterpriseid, userid, deviceid, manufacturer, model, os) \
      VALUES (?, ?, ?, ?, ?, ?)"
    )
    @addRevTag = @session.prepare(
      "INSERT INTO tags_rev (id, tag_text) VALUES (?,?)"
    )
    @addRevCat = @session.prepare(
      "INSERT INTO categories_rev (id, cat_text) VALUES (?,?)"
    )
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
    @eventsCallbacks = {
      'app.init' => ['createUserIfNotExists', 'updateUserLocation', 'updateUserPhoneDetails'],
      'app.login' => ['createUserIfNotExists', 'updateUserLocation', 'updateUserPhoneDetails'],
      'app.logout' => ['createUserIfNotExists', 'updateUserLocation', 'updateUserPhoneDetails'],
      'page.view' => ['createUserIfNotExists', 'createOrUpdatePage', 'updateUserLocation', 'updateUserPhoneDetails'],
      'productpage.view' => ['createUserIfNotExists', 'createOrUpdateProduct',
                             'updateUserLocation', 'updateUserPhoneDetails',
                             'productPageViewCallback'
    ],
      'update.push_token' => ['updateEnterprisePushKey', 'updateUserPushToken']
    }
  end

  def setupObservers
    @productRecommenderObserver = ProductRecommender::Observer.new
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

  # Create a user if not exists
  def createUserIfNotExists(msg)
    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    args = [eid, msg[:userId].to_i]
    result = @session.execute(@selectUser, arguments: args)
    if result.size == 0
      args.concat([Time.now])
      @session.execute(@createUser, arguments: args)
    end
  end

  # Gets the IDs for the tags specified.
  def getIdsForTags(eid, tags)

    generator = Cassandra::Uuid::Generator.new
    res = @session.execute(@selectTags, arguments: [eid, tags])
    tagids = {}

    # fetch all tag ids from db
    if res.length > 0
      res.each do |r|
        tagids[r['tag_text']] = r['id']
      end
    end

    # create tag for new tags
    if tagids.length < tags.length
      _tagids = {}
      newTags = tags - tagids.keys
      batch = @session.batch
      batch2 = @session.batch
      newTags.each do |t|
        tagid = generator.uuid
        args = [eid, t, tagid, nil, Time.now]
        batch.add(@addTag, args)
        _tagids[t] = tagid

        # create reverse tags entry
        args = [tagid, t]
        batch2.add(@addRevTag, args)
      end
      @session.execute(batch)
      @session.execute(batch2)
      tagids.merge(_tagids)
    end

    tagVals = tagids.values.map { |x| Cassandra::Uuid.new(x.to_s) }

    Set.new(tagVals)
  end

  def getIdsForCategories(eid, categories)
    generator = Cassandra::Uuid::Generator.new
    args = [eid, categories]
    res = @session.execute(@selectCats, arguments: args)
    catids = {}


    # fetch all categories' ids from db
    if res.length > 0
      res.each do |r|
        catids[r['cat_text']] = r['id']
      end
    end

    # create categories table entry for new categories
    if catids.length != categories.length
      _catids = {}
      newCats = categories - catids.keys
      batch = @session.batch
      batch2 = @session.batch
      newCats.each do |c|
        catid = generator.uuid
        args = [eid, c, catid, nil, Time.now]
        batch.add(@addCat, args)
        _catids[c] = catid

        # add reverse category mapping
        args = [catid, c]
        batch2.add(@addRevCat, args)
      end
      @session.execute(batch)
      @session.execute(batch2)
      catids.merge(_catids)
    end

    Set.new(catids.values.map {|x| Cassandra::Uuid.new(x.to_s)})
  end

  # Create or updates product
  def createOrUpdateProduct(msg)

    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    pid = msg[:productId].to_i

    categories = getIdsForCategories(eid, msg[:categories])
    tags = getIdsForTags(eid, msg[:tags])

    # check if the product already exists
    args = [eid, pid]

    result = @session.execute(@selectProductStatement, arguments: args)
    if result.size == 0
      product_msg = {
        enterpriseId: eid,
        id:           msg[:productId].to_i,
        categories:   categories,
        name:         msg[:productName],
        price:        msg[:price].to_f.round(2),
        tags:         tags
      }
      args = product_msg.values
      @session.execute(@createProductStatement, arguments: args)
      addProductsCallback(msg)
    elsif result.size == 1
      # if already exists, find if any attribute has changed and update it
      prod = result.first
      argCols = []

      if prod['name'] != msg[:productName]
        args << msg[:productName]
        argCols << 'name'
      end

      # CAUTION: KNOWN BUG
      # The price is not updated in the products table for some
      # weird reason where cassandra driver is unable to update
      # the float values. However, the latest values are always
      # updated in the productpage_view table
      if prod['price'].to_f.round(2) != msg[:price].to_f.round(2)
        args << msg[:price].to_f.round(2)
        argCols << 'price'
      end

      if prod['tags'] != tags
        args << tags
        argCols << 'tags'
      end

      if prod['categories'] != categories
        args << categories
        argCols << 'categories'
      end

      if argCols.length > 0
        updatedCols = argCols.map { |x| x.to_s + " = ?" }.join(', ')
        cql = "UPDATE products SET #{ updatedCols } \
        WHERE enterpriseid = ? AND id = ?"
        begin
          @session.execute(cql, arguments: args.rotate(2))
        rescue Exception => e
          puts e.inspect
        end
      end
    end
    pid
  end

  # Create or update a page
  def createOrUpdatePage(msg)
    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    routeUrl = msg[:routeUrl]

    categories = getIdsForCategories(eid, msg[:categories])
    tags = getIdsForTags(eid, msg[:tags])

    # check if the product already exists
    args = [eid, routeUrl]

    result = @session.execute(@selectPage, arguments: args)
    if result.size == 0
      args = [eid, routeUrl, categories, Time.now, tags]
      @session.execute(@createPage, arguments: args)
    elsif result.size == 1
      # if already exists, find if any attribute has changed and update it
      prod = result.first
      argCols = []

      if prod['tags'] != tags
        args << tags
        argCols << 'tags'
      end

      if prod['categories'] != categories
        args << categories
        argCols << 'categories'
      end

      if argCols.length > 0
        updatedCols = argCols.map { |x| x.to_s + " = ?" }.join(', ')
        cql = "UPDATE pages SET #{ updatedCols } \
        WHERE enterpriseid = ? AND routeUrl = ?"
        @session.execute(cql, arguments: args.rotate(2))
      end
    end
    routeUrl
  end

  # Callbacks to be executed on product pageview happening
  def productPageViewCallback(msg)
    eid, uid, pid = msg.values_at(:enterpriseId, :userId, :productId)

    # Currently there is only one callback.
    # More can be added here as the need be
    @productRecommenderObserver.registerUserProductView(eid, uid, pid)
  end

  # Callbacks to be executed on new product being added
  def addProductsCallback(msg)

    # Register this product with the tags
    unless msg[:tags].empty?
      msg[:tags].each do |tag_text|
        @productRecommenderObserver.registerProductForTag(eid, tag_text, pid)
      end
    end

    # Register this product with the categories
    unless msg[:categories].nil?
      msg[:categories].each do |cat_text|
        @productRecommenderObserver.registerProductForCategory(eid, cat_text,
                                                                pid)
      end
    end

  end

  # Updates user location
  def updateUserLocation(msg)
    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    uid = msg[:userId].to_i
    latitude = msg.fetch(:phone, {}).fetch('latitude', nil)
    longitude = msg.fetch(:phone, {}).fetch('longitude', nil)

    if latitude != nil and longitude != nil
      args = [eid, uid, Time.now, latitude.to_f.round(2), longitude.to_f.round(2)]
      @session.execute(@updateUserLocation, arguments: args)
    end
  end

  # Update user's phone details
  def updateUserPhoneDetails(msg)
    eid = Cassandra::Uuid.new(msg[:enterpriseId])
    uid = msg[:userId].to_i
    phone = msg.fetch(:phone, {})
    deviceId = phone.fetch('deviceId', nil)
    manufacturer = phone.fetch('manufacturer', nil)
    model = phone.fetch('model', nil)
    os = phone.fetch('os', nil)

    args = [eid, uid, deviceId, manufacturer, model, os]
    @session.execute(@updateUserPhone, arguments: args)
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
    elsif stmt.class == Cassandra::Statements::Prepared
      begin
        msgParsed.delete(:phone)
        msgParsed.delete(:categories)
        msgParsed.delete(:tags)
        msgParsed.delete(:routeUrl)
        msgParsed.delete(:productName)
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
