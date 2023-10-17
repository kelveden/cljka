(ns cljka.kafka
  (:require [cljka.config :refer [normalize-kafka-config]])
  (:import (java.util HashMap)
           (org.apache.kafka.clients.admin AdminClient)))

; TODO: move to core namespace along with all other configuration merging logic
(defn ->topic-name
  [{:keys [topics]} topic]
  (if (keyword? topic)
    (get-in topics [topic :name])
    topic))

(defn new-admin-client
  [kafka-config]
  (-> kafka-config
      ^HashMap (normalize-kafka-config)
      (AdminClient/create)))

(defn- wait-for-kafka-future
  [f]
  (deref f 3000 nil))

(defn get-topic-partitions
  "Gets a vector of partitions available for the given topic."
  [^AdminClient kafka-admin-client topic]
  (let [[_ fut] (-> (.describeTopics kafka-admin-client [topic])
                    (.topicNameValues)
                    (first))]
    (some->> (wait-for-kafka-future fut)
             (.partitions)
             (map #(.partition %))
             (vec))))

(defn get-group-offsets
  "Gets the offsets of the given consumer group."
  [^AdminClient kafka-admin-client group-id]
  (let [fut (-> (.listConsumerGroupOffsets kafka-admin-client ^String group-id)
                (.partitionsToOffsetAndMetadata))]
    (some->> (wait-for-kafka-future fut)
             ; Convert to map of topic->vec of partition/offset pairs
             (reduce (fn [acc [topic-partition offset-metadata]]
                       (let [topic     (.topic topic-partition)
                             partition (.partition topic-partition)
                             offset    (.offset offset-metadata)]
                         (update acc topic conj [partition offset])))
                     {})
             ; Ensure that the partition/offset pairs are sorted by partition
             (map (fn [[topic partition-offsets]]
                    [topic (->> partition-offsets
                                (sort-by first)
                                (vec))]))
             (into {}))))
