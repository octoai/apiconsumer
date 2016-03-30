require 'octocore'
require 'hooks'

module Octo
  module Consumer
    class Callbacks
      include Hooks

      define_hook :after_productpage_view

      after_productpage_view :update_trend_counters

      class << self

        def update_trend_counters(product, categories, tags)
          Octo::ProductHit.increment_for(product)
          categories.each do |cat|
            Octo::CategoryHit.increment_for cat
          end
          tags.each do |tag|
            Octo::TagHit.increment_for tag
          end
        end

      end
    end
  end
end
