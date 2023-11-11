(ns cljka.config-test
  (:require [clojure.test :refer :all]
            [cljka.config :refer [->kafka-config with-principal ->topic-name]]))


(deftest can-load-kafka-config-by-topic
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

(deftest can-load-kafka-config-by-principal
  (let [config {:environments {:my-env {:kafka      {:core-field1 "core-value1"
                                                     :core-field2 "core-value2"}
                                        :principals {:my-user {:kafka {:user-field1 "value1"
                                                                       :user-field2 "value2"}}}
                                        :topics     {:topic1 {:principal :my-user}}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value1"
            "user-field2" "value2"}
           (with-principal
             :my-user
             (->kafka-config config :my-env :topic1))))))

(deftest kafka-config-defaults-to-core
  (let [config {:environments {:my-env {:kafka {:core-field1 "core-value1"
                                                :core-field2 "core-value2"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"}
           (->kafka-config config :my-env "mytopic")))))

(deftest can-translate-keyword-topic-to-name
  (let [config {:environments {:my-env {:topics {:topic1 {:name "mytopic"}}}}}]
    (is (= "mytopic" (->topic-name config :my-env :topic1)))))

(deftest string-topic-is-not-translated
  (is (= "mytopic" (->topic-name {} :my-env "mytopic"))))