(ns cljka.kafka-test
  (:require [cljka.deserialization]
            [cljka.kafka :as kafka]
            [cljka.test-utils :refer [*kafka-admin-client* ensure-topic! produce! with-consumer with-kafka with-producer]]
            [clojure.test :refer :all]
            [taoensso.timbre :as log]
            [tick.core :as t])
  (:import (java.time Duration)
           (java.util UUID)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.common.serialization StringDeserializer)))

(log/set-config! (-> log/default-config
                     (merge {:min-level :warn})))

(use-fixtures :once with-kafka)


;------------------------------------------------
(comment kafka/->topic-name)
;------------------------------------------------


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


;------------------------------------------------
(comment kafka/get-partitions)
;------------------------------------------------


(deftest get-topic-partitions-returns-vector-of-partitions-for-topic
  (let [topic (str (UUID/randomUUID))]
    (ensure-topic! topic 6)

    (is (= [0 1 2 3 4 5]
           (kafka/get-partitions *kafka-admin-client* topic)))))


;------------------------------------------------
(comment kafka/get-group-offsets)
;------------------------------------------------


(deftest can-get-group-offsets-by-topic
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

    (let [group-offsets (kafka/get-group-offsets *kafka-admin-client* consumer-group)]
      ; THEN each topic has an entry per partition, ordered by partition
      (is (= [0 1 2 3] (vec (map first (-> group-offsets (get topic1))))))
      (is (= [0 1] (vec (map first (-> group-offsets (get topic2))))))

      ; AND each topic's offsets add up to the number of messages consumed by the consumer
      (let [topic-offsets (get group-offsets topic1)]
        (is (= 5 (reduce + (map second topic-offsets)))))

      (let [topic-offsets (get group-offsets topic2)]
        (is (= 5 (reduce + (map second topic-offsets))))))))

(deftest getting-group-offsets-returns-empty-map-if-consumer-group-unassigned
  (let [consumer-group (str (UUID/randomUUID))]
    (is (= {} (kafka/get-group-offsets *kafka-admin-client* consumer-group)))))


;------------------------------------------------
(comment kafka/get-offsets-at)
;------------------------------------------------


(deftest can-get-topic-starting-offsets
  ; GIVEN a topic
  (let [topic           (str (UUID/randomUUID))
        partition-count 2]
    (ensure-topic! topic partition-count)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 10 #(produce! producer topic (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN the start offsets of the topic are requested
    (let [topic-offsets (kafka/get-offsets-at *kafka-admin-client* topic :start)]
      ; THEN the offsets for all partitions are 0
      (is (= (->> (range partition-count)
                  (map #(vector % 0))
                  (vec))
             topic-offsets)))))

(deftest can-get-topic-end-offsets
  ; GIVEN a topic
  (let [topic           (str (UUID/randomUUID))
        partition-count 2]
    (ensure-topic! topic partition-count)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 10 #(produce! producer topic (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN the end offsets of the topic are requested
    (let [topic-offsets (kafka/get-offsets-at *kafka-admin-client* topic :end)]
      ; THEN the offsets add up to the total number of messages
      (is (= partition-count (count topic-offsets)))
      (is (= 10 (->> topic-offsets (map second) (reduce +)))))))

(deftest can-get-topic-offsets-at-specific-time
  ; GIVEN a topic with a single partition
  (let [topic           (str (UUID/randomUUID))
        partition-count 1]
    (ensure-topic! topic partition-count)

    (with-producer
      (fn [producer]
        (let [start (.toEpochMilli (t/instant))]
          ; AND 2 messages on the topic, produced 500ms apart from now.
          (produce! producer topic (str (UUID/randomUUID)) (str (UUID/randomUUID)))
          (Thread/sleep 500)
          (produce! producer topic (str (UUID/randomUUID)) (str (UUID/randomUUID)))

          ; WHEN the offset of the topic is requested at a time that should be BEFORE the message has been produced.
          (let [topic-offsets (kafka/get-offsets-at *kafka-admin-client* topic 0)]
            ; THEN the offset will be 0
            (is (= [[0 0]] topic-offsets)))

          ; AND WHEN the offset of the topic is requested at a time that should be AFTER the 1st message has been produced (but before the second).
          (let [topic-offsets (kafka/get-offsets-at *kafka-admin-client* topic (+ start 300))]
            ; THEN the offset will be 1 - i.e. the earliest offset with a timestamp greater than "at"
            (is (= [[0 1]] topic-offsets))))))))


;------------------------------------------------
(comment kafka/get-lag)
;------------------------------------------------


(deftest can-get-lag
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 100 #(produce! producer topic
                                                       (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN a consumer starts consuming
    (with-consumer StringDeserializer StringDeserializer consumer-group
                   (fn [^KafkaConsumer consumer]
                     (.subscribe consumer [topic])
                     (.poll consumer (Duration/ofSeconds 1))))

    ; THEN the lag is 0
    (is (= {:total 0 :partitions [[0 0] [1 0] [2 0] [3 0]]}
           (kafka/get-lag *kafka-admin-client* topic consumer-group)))))

(deftest no-lag-data-is-returned-if-consumer-group-not-consumed-from-topic
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 100 #(produce! producer topic
                                                       (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN a consumer initialises but doesn't actually consume
    (with-consumer StringDeserializer StringDeserializer consumer-group
                   (fn [^KafkaConsumer consumer]
                     (.subscribe consumer [topic])))

    ; THEN the lag cannot be calculated because the consumer has not yet started consuming
    (is (= :no-lag-data (kafka/get-lag *kafka-admin-client* topic consumer-group)))))


;------------------------------------------------
(comment kafka/set-group-offsets!)
;------------------------------------------------


(deftest can-set-consumer-group-offsets-to-start-of-topic
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 100 #(produce! producer topic
                                                       (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN a consumer starts consuming
    (with-consumer StringDeserializer StringDeserializer consumer-group
                   (fn [^KafkaConsumer consumer]
                     (.subscribe consumer [topic])
                     (.poll consumer (Duration/ofSeconds 1))))

    ; THEN the group offsets are at the end of topic
    (let [offsets (kafka/get-group-offsets *kafka-admin-client* consumer-group)]
      (is (= 100 (->> (get offsets topic) (map second) (reduce +)))))

    ; AND WHEN the consumer group offsets are reset to the start
    (kafka/set-group-offsets! *kafka-admin-client* topic consumer-group :start)

    ; THEN the group offsets output reflects the start of the topic
    (let [offsets (kafka/get-group-offsets *kafka-admin-client* consumer-group)]
      (is (= [[0 0] [1 0] [2 0] [3 0]] (->> offsets (map second) (reduce +)))))))

(deftest can-set-consumer-group-offsets-to-end-of-topic
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 100 #(produce! producer topic
                                                       (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN the group offsets are set to the end of the topic
    (kafka/set-group-offsets! *kafka-admin-client* topic consumer-group :end)

    ; AND a consumer starts consuming
    (with-consumer StringDeserializer StringDeserializer consumer-group
                   (fn [^KafkaConsumer consumer]
                     (.subscribe consumer [topic])

                     ; THEN no messages are retrieved
                     (is (empty? (.poll consumer (Duration/ofSeconds 1))))))

    ; AND the group offsets are at the end of topic
    (let [offsets (kafka/get-group-offsets *kafka-admin-client* consumer-group)]
      (is (= 100 (->> (get offsets topic) (map second) (reduce +)))))))

(deftest can-set-consumer-group-offsets-to-specific-offset
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (doall (repeatedly 100 #(produce! producer topic
                                                       (str (UUID/randomUUID)) (str (UUID/randomUUID)))))))

    ; WHEN the group offsets are set to the end of the topic
    (kafka/set-group-offsets! *kafka-admin-client* topic consumer-group 10)

    ; AND a consumer starts consuming
    (with-consumer StringDeserializer StringDeserializer consumer-group
                   (fn [^KafkaConsumer consumer]
                     (.subscribe consumer [topic])

                     ; THEN only those messages from after the offset are retrieved
                     (is (= 60 (-> (.poll consumer (Duration/ofSeconds 1))
                                   (.count))))))))