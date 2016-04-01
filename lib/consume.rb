require 'json'
require 'set'

require_relative 'callbacks'

module Octo
  module Consumer

    class EventsConsumer

      include Hooks

      # These are all the events which are allowed for an enterprise
      VALID_EVENTS = %w(app.init app.login app.logout
page.view productpage.view update.push_token)

      # These events are a sub set of above events.
      # The client is charged on these events and hence their
      # counter needs to be maintained
      API_EVENTS = %w(app.init app.login app.logout page.view productpage.view)

      def initialize()

      end

      def handle(msg)
        msg = parse(msg)
        eventName = msg.delete(:event_name)

        if VALID_EVENTS.include?eventName
          enterprise = checkEnterprise(msg)

          hook_opts = {}

          if API_EVENTS.include?eventName
            hook_opts[:event] = register_api_event(enterprise, eventName)
          end

          case eventName
            when 'app.init'
            user = checkUser(enterprise, msg)
            Octo::AppInit.new(enterprise: enterprise,
                              created_at: Time.now,
                              userid: user.id,
                              customid: msg[:uuid]).save!
            updateLocationHistory(user, msg)
            updateUserPhoneDetails(user, msg)
            call_hooks(eventName, hook_opts)
          when 'app.login'
            user = checkUser(enterprise, msg)
            Octo::AppLogin.new(enterprise: enterprise,
                               created_at: Time.now,
                               userid: user.id,
                              customid: msg[:uuid]).save!
            updateLocationHistory(user, msg)
            updateUserPhoneDetails(user, msg)
            call_hooks(eventName, hook_opts)
          when 'app.logout'
            user = checkUser(enterprise, msg)
            event = Octo::AppLogout.new(enterprise: enterprise,
                                created_at: Time.now,
                                userid: user.id,
                              customid: msg[:uuid]).save!
            updateLocationHistory(user, msg)
            updateUserPhoneDetails(user, msg)
            call_hooks(eventName, hook_opts)
          when 'page.view'
            user = checkUser(enterprise, msg)
            checkPage(enterprise, msg)
            updateLocationHistory(user, msg)
            updateUserPhoneDetails(user, msg)
            call_hooks(eventName, hook_opts)
          when 'productpage.view'
            user = checkUser(enterprise, msg)
            product, categories, tags = checkProduct(enterprise, msg)
            updateLocationHistory(user, msg)
            updateUserPhoneDetails(user, msg)
            hook_opts.merge!({
                product: product,
                categories: categories,
                tags: tags })
            call_hooks(eventName, hook_opts)
          when 'update.push_token'
            user = checkUser(enterprise, msg)
            checkPushToken(enterprise, user, msg)
            checkPushKey(enterprise, msg)
          end
        end
      end

      private

      def register_api_event(enterprise, event_name)
        Octo::ApiEvent.findOrCreate({ enterprise_id: enterprise.id,
                                    eventname: event_name})
      end

      def call_hooks(event, *args)
        hook = [:after, event.gsub('.', '_')].join('_').to_sym
        Octo::Consumer::Callbacks.run_hook(hook, *args)
      end

      # Checks for push tokens and creates or updates it
      # @param [Octo::Enterprise] enterprise The Enterprise object
      # @param [Octo::User] user The user to whom this token belongs to
      # @param [Hash] msg The message hash
      # @return [Octo::PushToken] The push token object corresponding to this user
      def checkPushToken(enterprise, user, msg)
        args = {
          user_id: user.id,
          user_enterprise_id: enterprise.id,
          push_type: msg[:pushType].to_i
        }
        opts = {
          pushtoken: msg[:pushToken]
        }
        Octo::PushToken.findOrCreateOrUpdate(args, opts)
      end

      # Checks for push keys and creates or updates it
      # @param [Octo::Enterprise] enterprise The Enterprise object
      # @param [Hash] msg The message hash
      # @return [Octo::PushKey] The push key object corresponding to this user
      def checkPushKey(enterprise, msg)
        args = {
          enterprise_id: enterprise.id,
          push_type: msg[:pushType].to_i
        }
        opts = {
          key: msg[:pushKey]
        }
        Octo::PushKey.findOrCreateOrUpdate(args, opts)
      end

      # Check if the enterprise exists. Create a new enterprise if it does
      #   not exist. This method makes sense because the enterprise authentication
      #   is handled by kong. Hence we can be sure that all these enterprises
      #   are valid.
      # @param [Hash] msg The message hash
      # @return [Octo::Enterprise] The enterprise object
      def checkEnterprise(msg)
        Octo::Enterprise.findOrCreate({id: msg[:enterpriseId]},
                                      {name: msg[:enterpriseName]})
      end

      # Checks for user and creates if not exists
      # @param [Octo::Enterprise] enterprise The Enterprise object
      # @param [Hash] msg The message hash
      # @return [Octo::User] The push user object corresponding to this user
      def checkUser(enterprise, msg)
        args = {
          enterprise_id: enterprise.id,
          id: msg[:userId]
        }
        Octo::User.findOrCreate(args)
      end

      # Updates location for a user
      # @param [Octo::User] user The user to whom this token belongs to
      # @param [Hash] msg The message hash
      # @return [Octo::UserLocationHistory] The location history object
      #   corresponding to this user
      def updateLocationHistory(user, msg)
        Octo::UserLocationHistory.new(
          user: user,
          latitude: msg[:phone].fetch('latitude', 0.0),
          longitude: msg[:phone].fetch('longitude', 0.0),
          created_at: Time.now
        ).save!
      end

      # Updates user's phone details
      # @param [Octo::User] user The user to whom this token belongs to
      # @param [Hash] msg The message hash
      # @return [Octo::UserPhoneDetails] The phone details object
      #   corresponding to this user
      def updateUserPhoneDetails(user, msg)
        args = {user_id: user.id, user_enterprise_id: user.enterprise.id}
        opts = {deviceid: msg[:phone].fetch('deviceId', ''),
                manufacturer: msg[:phone].fetch('manufacturer', ''),
                model: msg[:phone].fetch('model', ''),
                os: msg[:phone].fetch('os', '')}
        Octo::UserPhoneDetails.findOrCreateOrUpdate(args, opts)
      end

      # Checks the existence of a page and creates if not found
      # @param [Octo::Enterprise] enterprise The Enterprise object
      # @param [Hash] msg The message hash
      # @return [Array<Octo::Page, Array<Octo::Category>, Array<Octo::Tag>] The
      #   page object, array of categories objects and the array of tags
      #   object
      def checkPage(enterprise, msg)
        cats = checkCategories(enterprise, msg[:categories])
        tags = checkTags(enterprise, msg[:tags])

        args = {
          enterprise_id: enterprise.id,
          routeUrl: msg[:routeUrl]
        }
        opts = {
          categories: Set.new(msg[:categories]),
          tags: Set.new(msg[:tags],
          customid: msg[:uuid])
        }
        page = Octo::Page.findOrCreateOrUpdate(args, opts)
        [page, cats, tags]
      end

      # Checks for existence of a product and creates if not found
      # @param [Octo::Enterprise] enterprise The Enterprise object
      # @param [Hash] msg The message hash
      # @return [Array<Octo::Product, Array<Octo::Category>, Array<Octo::Tag>] The
      #   product object, array of categories objects and the array of tags
      #   object
      def checkProduct(enterprise, msg)
        categories = checkCategories(enterprise, msg[:categories])
        tags = checkTags(enterprise, msg[:tags])

        args = {
          enterprise_id: enterprise.id,
          id: msg[:productId]
        }
        opts = {
          categories: Set.new(msg[:categories]),
          tags: Set.new(msg[:tags]),
          price: msg[:price].to_f,
          name: msg[:productName],
          routeurl: msg[:routeUrl],
          customid: msg[:uuid]
        }
        prod = Octo::Product.findOrCreateOrUpdate(args, opts)
        [prod, categories, tags]
      end

      # Checks for categories and creates if not found
      # @param [Octo::Enterprise] enterprise The enterprise object
      # @param [Array<String>] categories An array of categories to be checked
      # @return [Array<Octo::Category>] An array of categories object
      def checkCategories(enterprise, categories)
        categories.collect do |category|
          Octo::Category.findOrCreate({enterprise_id: enterprise.id,
                                       cat_text: category})
        end
      end

      # Checks for tags and creates if not found
      # @param [Octo::Enterprise] enterprise The enterprise object
      # @param [Array<String>] categories An array of tags to be checked
      # @return [Array<Octo::Tag>] An array of categories object
      def checkTags(enterprise, tags)
        tags.collect do |tag|
          Octo::Tag.findOrCreate({enterprise_id: enterprise.id, tag_text: tag})
        end
      end

      def parse(msg)
        msg2 = JSON.parse(msg)
        msg = msg2
        m = {
          id:             msg['uuid'],
          enterpriseId:   msg['enterprise']['id'],
          enterpriseName: msg['enterprise']['customId'],
          event_name:     msg['event_name'],
          phone:          msg.fetch('phoneDetails', {}),
          userId:         msg.fetch('userId', -1),
          created_at:     Time.now
        }
        case msg['event_name']
        when 'page.view'
          m.merge!({
            routeUrl:     msg['routeUrl'],
            categories:   msg['categories'],
            tags:         msg['tags']
          })
        when 'productpage.view'
          m.merge!({
            routeUrl:     msg['routeUrl'],
            categories:   msg['categories'],
            tags:         msg['tags'],
            productId:    msg['productId'],
            productName:  msg['productName'],
            price:        msg['price']
          })
        when 'update.push_token'
          m.merge!({
            pushType:     msg['notificationType'],
            pushKey:      msg['pushKey'],
            pushToken:    msg['pushToken']
          })
        end
        return m
      end
    end
  end
end
