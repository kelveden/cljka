(ns cljka.kafka-test
  #_{:clj-kondo/ignore [:refer-all]}
  (:require [cljka.kafka :as kafka]
            [cljka.test-utils :refer [*kafka-admin-client* *kafka-config* ensure-topic! produce! with-consumer with-kafka with-producer]]
            [clojure.set :refer [subset?]]
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

(defn- generate-random-messages
  [n]
  (repeatedly n #(vector (str (UUID/randomUUID))
                         (str (UUID/randomUUID)))))

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
                     (produce! producer topic1 (generate-random-messages 5))
                     (produce! producer topic2 (generate-random-messages 5))))

    ; WHEN a consumer consumes 5 messages from across both topics
    (with-consumer StringDeserializer StringDeserializer consumer-group
      (fn [^KafkaConsumer consumer]
        (.subscribe consumer [topic1 topic2])
        (doall (repeatedly 5 #(.poll consumer (Duration/ofSeconds 1))))))

    (let [group-offsets-topic1 (kafka/get-group-offsets *kafka-admin-client* topic1 consumer-group)
          group-offsets-topic2 (kafka/get-group-offsets *kafka-admin-client* topic2 consumer-group)]
      ; THEN each topic has an entry per partition, ordered by partition
      (is (= [0 1 2 3] (vec (map first group-offsets-topic1))))
      (is (= [0 1] (vec (map first group-offsets-topic2))))

      ; AND each topic's offsets add up to the number of messages consumed by the consumer
      (is (= 5 (reduce + (map second group-offsets-topic1))))
      (is (= 5 (reduce + (map second group-offsets-topic2)))))))

(deftest getting-group-offsets-returns-empty-vector-if-consumer-group-unassigned
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (is (= [] (kafka/get-group-offsets *kafka-admin-client* topic consumer-group)))))

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
                     (produce! producer topic (generate-random-messages 10))))

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
                     (produce! producer topic (generate-random-messages 10))))

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
          (produce! producer topic [[(str (UUID/randomUUID)) (str (UUID/randomUUID))]])
          (Thread/sleep 500)
          (produce! producer topic [[(str (UUID/randomUUID)) (str (UUID/randomUUID))]])

          ; WHEN the offset of the topic is requested at a time that should be BEFORE the message has been produced.
          (let [topic-offsets (kafka/get-offsets-at *kafka-admin-client* topic 0)]
            ; THEN the offset will be 0
            (is (= [[0 0]] topic-offsets)))

          ; AND WHEN the offset of the topic is requested at a time that should be AFTER the 1st message has been produced (but before the second).
          (let [topic-offsets (kafka/get-offsets-at *kafka-admin-client* topic (+ start 300))]
            ; THEN the offset will be 1 - i.e. the earliest offset with a timestamp greater than "at"
            (is (= [[0 1]] topic-offsets))))))))

;------------------------------------------------
(comment kafka/get-offset-at)
;------------------------------------------------

(deftest can-get-topic-starting-offset
  ; GIVEN a topic
  (let [topic           (str (UUID/randomUUID))
        partition-count 2]
    (ensure-topic! topic partition-count)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (produce! producer topic (generate-random-messages 10))))

    ; WHEN the start offsets of the topic are requested
    (let [topic-offset1 (kafka/get-offset-at *kafka-admin-client* topic 0 :start)
          topic-offset2 (kafka/get-offset-at *kafka-admin-client* topic 1 :start)]
      ; THEN the offset for all partitions are 0
      (is (= 0 topic-offset1 topic-offset2)))))

(deftest can-get-topic-end-offset
  ; GIVEN a topic
  (let [topic           (str (UUID/randomUUID))
        partition-count 2]
    (ensure-topic! topic partition-count)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (produce! producer topic (generate-random-messages 10))))

    ; WHEN the end offsets of the topic are requested
    (let [topic-offset1 (kafka/get-offset-at *kafka-admin-client* topic 0 :end)
          topic-offset2 (kafka/get-offset-at *kafka-admin-client* topic 1 :end)]
      ; THEN the offsets add up to the total number of messages
      (is (= 10 (+ topic-offset1 topic-offset2))))))

(deftest can-get-topic-offset-at-specific-time
  ; GIVEN a topic with a single partition
  (let [topic           (str (UUID/randomUUID))
        partition-count 1]
    (ensure-topic! topic partition-count)

    (with-producer
      (fn [producer]
        (let [start (.toEpochMilli (t/instant))]
          ; AND 2 messages on the topic, produced 500ms apart from now.
          (produce! producer topic [[(str (UUID/randomUUID)) (str (UUID/randomUUID))]])
          (Thread/sleep 500)
          (produce! producer topic [[(str (UUID/randomUUID)) (str (UUID/randomUUID))]])

          ; WHEN the offset of the topic is requested at a time that should be BEFORE the message has been produced.
          (let [topic-offset (kafka/get-offset-at *kafka-admin-client* topic 0 0)]
            ; THEN the offset will be 0
            (is (= 0 topic-offset)))

          ; AND WHEN the offset of the topic is requested at a time that should be AFTER the 1st message has been produced (but before the second).
          (let [topic-offset (kafka/get-offset-at *kafka-admin-client* topic 0 (+ start 300))]
            ; THEN the offset will be 1 - i.e. the earliest offset with a timestamp greater than "at"
            (is (= 1 topic-offset))))))))

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
                     (produce! producer topic (generate-random-messages 100))))

    ; WHEN a consumer starts consuming
    (with-consumer StringDeserializer StringDeserializer consumer-group
      (fn [^KafkaConsumer consumer]
        (.subscribe consumer [topic])
        (.poll consumer (Duration/ofSeconds 1))))

    ; THEN the lag is 0
    (is (= {:total 0 :by-partition [[0 0] [1 0] [2 0] [3 0]]}
           (kafka/get-lag *kafka-admin-client* topic consumer-group)))))

(deftest no-lag-data-is-returned-if-consumer-group-not-consumed-from-topic
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (produce! producer topic (generate-random-messages 100))))

    ; WHEN a consumer initialises but doesn't actually consume
    (with-consumer StringDeserializer StringDeserializer consumer-group
      (fn [^KafkaConsumer consumer]
        (.subscribe consumer [topic])))

    ; THEN the lag cannot be calculated because the consumer has not yet started consuming
    (is (= :no-lag-data (kafka/get-lag *kafka-admin-client* topic consumer-group)))))

;------------------------------------------------
(comment kafka/set-group-offsets!)
;------------------------------------------------

(deftest can-set-consumer-group-offsets-to-specific-offset
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (produce! producer topic (generate-random-messages 100))))

    ; WHEN the group offsets are set to the second message on each partition
    (kafka/set-group-offsets! *kafka-admin-client* topic consumer-group [[0 1] [1 1] [2 1] [3 1]])

    ; AND a consumer starts consuming
    (with-consumer StringDeserializer StringDeserializer consumer-group
      (fn [^KafkaConsumer consumer]
        (.subscribe consumer [topic])

                     ; THEN only those messages from after the offset are retrieved
        (is (= 96 (-> (.poll consumer (Duration/ofSeconds 1))
                      (.count))))))))

;------------------------------------------------
(comment kafka/set-group-offset!)
;------------------------------------------------

(deftest can-set-consumer-group-offset-to-specific-offset
  ; GIVEN a topic
  (let [topic          (str (UUID/randomUUID))
        consumer-group (str (UUID/randomUUID))]
    (ensure-topic! topic 4)

    ; AND some messages on the topic
    (with-producer (fn [producer]
                     (produce! producer topic (generate-random-messages 100))))

    ; WHEN a consumer group offset is reset to the end of the topic
    (kafka/set-group-offset! *kafka-admin-client* topic 3 consumer-group 10)
    (Thread/sleep 200)

    (let [offsets (kafka/get-group-offsets *kafka-admin-client* topic consumer-group)]
      ; THEN the group offsets output reflects the end of the topic for partition 3
      (is (= 1 (count offsets)))
      (is (= 3 (first (first offsets))))
      (is (= 10 (second (first offsets)))))))

;------------------------------------------------
(comment kafka/get-topics)
;------------------------------------------------

(deftest can-get-topics
  (let [topics (repeatedly 5 #(str (UUID/randomUUID)))]
    (doseq [topic topics] (ensure-topic! topic 1))

    (is (subset? (set topics)
                 (set (kafka/get-topics *kafka-admin-client*))))))

;------------------------------------------------
(comment kafka/start-consumer)
;------------------------------------------------

(defn- default-consumer-config
  []
  (merge *kafka-config*
         {:key.deserializer   StringDeserializer
          :value.deserializer StringDeserializer}))

(deftest can-consume-from-start-of-topic
  ; GIVEN a topic
  (let [topic    (str (UUID/randomUUID))
        messages (generate-random-messages 100)]
    (ensure-topic! topic 4)

    ; AND the messages are produced to the topic
    (with-producer (fn [producer]
                     (produce! producer topic messages)))

    ; WHEN consumption is started at the beginning of the topic.
    (with-open [consumer (-> (default-consumer-config)
                             (kafka/start-consumer topic :start))]
      (let [consumed (.poll consumer (Duration/ofSeconds 1))]
        ; THEN all messages are consumed
        (is (= (set messages)
               (set (->> consumed
                         (map #(vector (.key %) (.value %)))
                         (vec)))))))))

(deftest can-consume-from-end-of-topic
  ; GIVEN a topic
  (let [topic    (str (UUID/randomUUID))
        messages (generate-random-messages 100)]
    (ensure-topic! topic 4)

    ; AND the messages are produced to the topic
    (with-producer (fn [producer]
                     (produce! producer topic messages)))

    ; WHEN consumption is started at the end of the topic.
    (with-open [consumer (-> (default-consumer-config)
                             (kafka/start-consumer topic :end))]
      (let [consumed (.poll consumer (Duration/ofSeconds 0))]
        ; THEN no messages are consumed
        (is (empty? consumed))))))

(deftest can-consume-from-specific-partitions
  ; GIVEN a topic
  (let [topic    (str (UUID/randomUUID))
        messages (generate-random-messages 100)]
    (ensure-topic! topic 4)

    ; AND the messages are produced to the topic
    (with-producer (fn [producer]
                     (produce! producer topic messages)))

    ; WHEN consumption is started at a specific point on the topic.
    (with-open [consumer1 (-> (default-consumer-config)
                              (kafka/start-consumer topic [[0 :start] [1 :start]]))
                consumer2 (-> (default-consumer-config)
                              (kafka/start-consumer topic [[2 :start] [3 :start]]))]
      (let [consumed1 (.poll consumer1 (Duration/ofSeconds 1))
            consumed2 (.poll consumer2 (Duration/ofSeconds 1))]
        ; THEN all messages from the topic are consumed by individual partition
        (is (= 100 (+ (.count consumed1) (.count consumed2))))))))

(deftest can-consume-from-a-specific-point-in-time
  ; GIVEN a topic
  (let [topic     (str (UUID/randomUUID))
        messages1 (generate-random-messages 40)
        messages2 (generate-random-messages 60)]
    (ensure-topic! topic 4)

    ; AND the messages are produced to the topic
    (let [start (System/currentTimeMillis)]
      (with-producer (fn [producer]
                       (produce! producer topic messages1)
                       (Thread/sleep 500)
                       (produce! producer topic messages2)))

      ; WHEN consumption is started at a specific point on the topic.
      (with-open [consumer (-> (default-consumer-config)
                               (kafka/start-consumer topic (+ start 400)))]
        (let [consumed (.poll consumer (Duration/ofSeconds 1))]
          ; THEN all messages from the topic are consumed by individual partition
          (is (= 60 (.count consumed))))))))
