(ns cljka.config-test
  (:require [clojure.test :refer :all]
            [cljka.config :refer [->kafka-config with-principal]]))


(deftest principal-kafka-config-is-merged
  (let [config {:environments {:my-env {:kafka      {:core-field1 "core-value1"
                                                     :core-field2 "core-value2"}
                                        :principals {:my-user {:kafka {:user-field1 "value1"
                                                                       :user-field2 "value2"}}}
                                        :topics     {:topic1 {:name "sometopic"}}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value1"
            "user-field2" "value2"}
           (with-principal
             :my-user
             (->kafka-config config :my-env :topic1))))))

(deftest principal-by-topic-kafka-config-is-merged
  (let [config {:environments {:my-env {:kafka      {:core-field1 "core-value1"
                                                     :core-field2 "core-value2"}
                                        :principals {:my-user {:kafka {:user-field1 "value1"
                                                                       :user-field2 "value2"}}}
                                        :topics     {:topic1 {:principal :my-user}}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value1"
            "user-field2" "value2"}
           (->kafka-config config :my-env :topic1)))))

(deftest kafka-config-defaults-to-core
  (let [config {:environments {:my-env {:kafka {:core-field1 "core-value1"
                                                :core-field2 "core-value2"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"}
           (->kafka-config config :my-env "mytopic")))))
