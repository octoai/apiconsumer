require 'octocore'
require 'hooks'

module Octo
  module Consumer
    class Callbacks
      include Hooks

      define_hook :after_productpage_view, :after_app_init, :after_app_login, :after_app_logout, :after_page_view

      after_productpage_view :update_trend_counters

      after_app_init :update_events_counter
      after_app_login :update_events_counter
      after_app_logout :update_events_counter
      after_page_view :update_events_counter

      class << self

        def update_trend_counters(product, categories, tags)
          Octo::EventHit.increment_for(product)
          Octo::ProductHit.increment_for(product)
          categories.each do |cat|
            Octo::CategoryHit.increment_for cat
          end
          tags.each do |tag|
            Octo::TagHit.increment_for tag
          end
        end

        def update_events_counter(event)
          Octo::EventHit.increment_for(event)
        end

      end
    end
  end
end
