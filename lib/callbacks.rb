require 'octocore'
require 'hooks'

module Octo
  module Consumer
    class Callbacks
      include Hooks

      define_hook :after_productpage_view, :after_app_init, :after_app_login, :after_app_logout, :after_page_view

      after_app_init :update_counters
      after_app_login :update_counters
      after_app_logout :update_counters
      after_page_view :update_counters
      after_productpage_view :update_counters

      class << self

        def update_counters(obj)
          if obj[:product]
            Octo::ProductHit.increment_for(obj[:product])
          end
          if obj[:categories]
            obj[:categories].each do |cat|
              Octo::CategoryHit.increment_for cat
            end
          end
          if obj[:tags]
            obj[:tags].each do |tag|
              Octo::TagHit.increment_for tag
            end
          end
          if obj[:event]
            Octo::EventHit.increment_for(obj[:event])
          end
        end

      end
    end
  end
end
