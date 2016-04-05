require 'octocore'
require 'octorecommender'
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
      after_productpage_view :update_recommenders

      class << self

        # Updates the counters of various types depending
        #   on the event.
        # @param [Hash] opts The options hash
        def update_counters(opts)
          if opts.has_key?(:product)
            Octo::ProductHit.increment_for(opts[:product])
          end
          if opts.has_key?(:categories)
            opts[:categories].each do |cat|
              Octo::CategoryHit.increment_for(cat)
            end
          end
          if opts.has_key?(:tags)
            opts[:tags].each do |tag|
              Octo::TagHit.increment_for(tag)
            end
          end
          if opts.has_key?(:event)
            Octo::ApiHit.increment_for(opts[:event])
          end
        end

        # Updates for recommendations
        # @param [Hash] opts The options hash
        def update_recommenders(opts)
          user = opts[:user]
          product = opts[:product]

          if user and product
            recommender = Octo::Recommender.new
            recommender.register_user_product_view(user, product)
            recommender.register_user_action_time(user, Time.now.floor)
          end
        end

      end
    end
  end
end