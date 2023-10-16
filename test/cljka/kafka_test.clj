(ns cljka.kafka-test
  (:require [clojure.test :refer :all]
            [cljka.kafka :as kafka]
            [taoensso.timbre :as log]
            [cljka.test-utils :refer [with-kafka ensure-topic! *kafka-config*]])
  (:import (org.apache.kafka.common.serialization StringDeserializer)))

(log/set-config! (-> log/default-config
                     (merge {:min-level :warn})))

(use-fixtures :once with-kafka)

(deftest ->topic-name-converts-keyword-topic-to-string-from-config
  (is (= "some-topic2"
         (#'kafka/->topic-name {:topics {:topic1 {:name "some-topic1"}
                                         :topic2 {:name "some-topic2"}
                                         :topic3 {:name "some-topic3"}}}
           :topic2))))

(deftest ->topic-name-returns-string-topic-as-is
  (is (= "topic1"
         (#'kafka/->topic-name {:topics {:topic1 {:name "some-topic1"}}}
           "topic1"))))

(deftest ->topic-name-returns-nil-if-topic-not-found
  (is (nil? (#'kafka/->topic-name {:topics {}}
              :topic1))))

(deftest get-topic-partitions-returns-vector-of-partitions-for-topic
  (ensure-topic! "my-topic" 6)
  (let [env-config {:kafka  (-> *kafka-config*
                                (merge {:key.deserializer   StringDeserializer
                                        :value.deserializer StringDeserializer}))
                    :topics {:topic1 {:name "my-topic"}}}]
    (is (= [0 1 2 3 4 5]
           (kafka/get-topic-partitions env-config :topic1)))))

(deftest get-topic-partitions-returns-error-if-topic-not-found-in-config
  (let [env-config {}]
    (is (= :topic-not-configured
           (kafka/get-topic-partitions env-config :topic1)))))