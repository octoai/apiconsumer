require 'predictor'

Predictor.redis = Redis.new(:url => ENV["PREDICTOR_REDIS"],
                            :driver => :hiredis)


module ProductRecommender
  class Recommender
    include Predictor::Base

    SEPARATOR = '__'

    limit_similarities_to 128
    input_matrix :users, weight: 1.0
    input_matrix :tags, weight: 1.0
    input_matrix :categories, weight: 1.0

  end

  class Observer

    def initialize
      @recommender = Recommender.new
    end

    def registerUserProductView(enterpriseid, userid, productid)
      _uid = [enterpriseid, userid].join(Recommender::SEPARATOR)
      _pid = [enterpriseid, productid].join(Recommender::SEPARATOR)
#      @recommender.add_to_matrix(:users, userid, productid)
      @recommender.add_to_matrix(:users, _uid, _pid)
    end

    def registerProductForTag(enterpriseid, tag, productid)
      _pid = [enterpriseid, productid].join(Recommender::SEPARATOR)
      _tag = [enterpriseid, tag].join(Recommender:SEPARATOR)
#      @recommender.add_to_matrix(:tags, tag, productid)
      @recommender.add_to_matrix(:tags, _tag, _pid)
    end

    def registerProductForCategory(enterpriseid, category, productid)
      _pid = [enterpriseid, productid].join(Recommender::SEPARATOR)
      _cat = [enterpriseid, category].join(Recommender::SEPARATOR)
#      @recommender.add_to_matrix(:categories, category, productid)
      @recommender.add_to_matrix(:categories, _cat, _pid)
    end

  end

  class Scorer

    def self.perform
      recommender = Recommender.new
      recommender.process!
    end
  end

end
