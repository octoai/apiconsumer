require 'octocore'
require 'json'

module Octo
  class Consumer

    VALID_EVENTS = %w(app.init app.login app.logout page.view)

    def initialize()

    end

    def handle(msg)
      msg = parse(msg)
      eventName = msg.delete(:event_name)

      if VALID_EVENTS.include?eventName
        enterprise = checkEnterprise(msg)
        case eventName
        when 'app.init'
          user = checkUser(enterprise, msg)
          Octo::AppInit.new(enterprise: enterprise,
                            created_at: Time.now,
                            userid: user.id).save!
          updateLocationHistory(user, msg)
          updateUserPhoneDetails(user, msg)
        when 'app.login'
          user = checkUser(enterprise, msg)
          Octo::AppLogin.new(enterprise: enterprise,
                             created_at: Time.now,
                             userid: user.id).save!
          updateLocationHistory(user, msg)
          updateUserPhoneDetails(user, msg)
        when 'app.logout'
          user = checkUser(enterprise, msg)
          Octo::AppLogout.new(enterprise: enterprise,
                              created_at: Time.now,
                              userid: user.id).save!
          updateLocationHistory(user, msg)
          updateUserPhoneDetails(user, msg)
        when 'page.view'
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
        res = Octo::Enterprise.new(id: msg[:enterpriseId],
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

    def updateLocationHistory(user, msg)
      Octo::UserLocationHistory.new(
        user: user,
        latitude: msg[:phone].fetch('latitude', 0.0),
        longitude: msg[:phone].fetch('longitude', 0.0),
        created_at: Time.now
      ).save!
    end

    def updateUserPhoneDetails(user, msg)
      res = Octo::UserPhoneDetails.get_cached(user_id: user.id, user_enterprise_id: user.enterprise.id)
      unless res
        res = Octo::UserPhoneDetails.new(user_enterprise_id: user.enterprise.id,
                                         user_id: user.id,
                                         deviceid: msg[:phone].fetch('deviceId', ''),
                                         manufacturer: msg[:phone].fetch('manufacturer', ''),
                                         model: msg[:phone].fetch('model', ''),
                                         os: msg[:phone].fetch('os', '')).save!
      else

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
