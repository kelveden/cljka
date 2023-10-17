(ns cljka.kafka-test
  (:require [clojure.test :refer :all]
            [cljka.kafka :as kafka]
            [taoensso.timbre :as log]
            [cljka.test-utils :refer [with-kafka ensure-topic! *kafka-admin-client* with-consumer with-producer produce!]]
            [cljka.deserialization])
  (:import (java.time Duration)
           (java.util UUID)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.common.serialization StringDeserializer)))

(log/set-config! (-> log/default-config
                     (merge {:min-level :warn})))

(use-fixtures :once with-kafka)

; TODO --- START move with ->topic-name
(deftest ->topic-name-converts-keyword-topic-to-string-from-config
  (is (= "some-topic2"
         (kafka/->topic-name {:topics {:topic1 {:name "some-topic1"}
                                       :topic2 {:name "some-topic2"}
                                       :topic3 {:name "some-topic3"}}}
                             :topic2))))

(deftest ->topic-name-returns-string-topic-as-is
  (is (= "topic1"
         (kafka/->topic-name {:topics {:topic1 {:name "some-topic1"}}}
                             "topic1"))))

(deftest ->topic-name-returns-nil-if-topic-not-found
  (is (nil? (kafka/->topic-name {:topics {}}
                                :topic1))))
; TODO --- END

(deftest get-topic-partitions-returns-vector-of-partitions-for-topic
  (let [topic (str (UUID/randomUUID))]
    (ensure-topic! topic 6)

    (is (= [0 1 2 3 4 5]
           (kafka/get-topic-partitions *kafka-admin-client* topic)))))

(deftest get-consumer-group-offsets-gets-offsets-by-topic
  ; GIVEN two topics
  (let [topic1         (str (UUID/randomUUID))
        topic2         (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic1 4)
    (ensure-topic! topic2 2)

    ; AND 5 messages on each topic
    (with-producer (fn [producer]
                     (doall (repeatedly 5 #(produce! producer topic1 (str (UUID/randomUUID)) (str (UUID/randomUUID)))))
                     (doall (repeatedly 5 #(produce! producer topic2 (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN a consumer consumes 5 messages from across both topics
    (with-consumer StringDeserializer StringDeserializer consumer-group
                   (fn [^KafkaConsumer consumer]
                     (.subscribe consumer [topic1 topic2])
                     (doall (repeatedly 5 #(.poll consumer (Duration/ofSeconds 1))))))

    ; THEN the group offsets are defined by topic
    (let [group-offsets (kafka/get-group-offsets *kafka-admin-client* consumer-group)]
      (is (= #{topic1 topic2} (set (keys group-offsets))))

      ; AND each topic has an entry per partition, ordered by partition
      (is (= [0 1 2 3] (vec (map first (-> group-offsets (get topic1))))))
      (is (= [0 1] (vec (map first (-> group-offsets (get topic2))))))

      ; AND each topic's offsets add up to the number of messages consumed by the consumer
      (let [topic-offsets (get group-offsets topic1)]
        (is (= 5 (reduce + (map second topic-offsets)))))

      (let [topic-offsets (get group-offsets topic2)]
        (is (= 5 (reduce + (map second topic-offsets))))))))

(deftest get-consumer-group-offsets-returns-empty-map-if-consumer-group-unassigned
  (let [consumer-group (str (UUID/randomUUID))]
    (is (= {} (kafka/get-group-offsets *kafka-admin-client* consumer-group)))))