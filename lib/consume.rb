require 'octocore'
require 'json'

module Octo
  class Consumer

    VALID_EVENTS = %w(app.init )

    def initialize()

    end

    def handle(msg)
      msg = parse(msg)
      eventName = msg.delete(:event_name)

      if VALID_EVENTS.include?eventName
        enterprise = checkEnterprise(msg)
        case eventName
        when 'app.init'
          checkUser(enterprise, msg)
        end
      end
    end

    private

    # Check if the enterprise exists. Create a new enterprise if it does
    #   not exist. This method makes sense because the enterprise authentication
    #   is handled by kong. Hence we can be sure that all these enterprises
    #   are valid.
    # @param [Hash] msg The message hash
    def checkEnterprise(msg)
      res = Octo::Enterprise.get_cached(id: msg[:enterpriseId])
      unless res
        res = Octo::Enterprise.new(
              id: msg[:enterpriseId],
              name: msg[:enterpriseName]
            ).save!
      end
      res
    end

    def checkUser(enterprise, msg)
      res = Octo::User.get_cached(enterprise_id: enterprise.id,
                                  id: msg[:userId])
      unless res
        res = Octo::User.new(
          enterprise_id: enterprise.id,
          id: msg[:userId]).save!
      end
      res
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
