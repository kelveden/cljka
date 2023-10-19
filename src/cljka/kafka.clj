(ns cljka.kafka
  (:require [cljka.config :refer [normalize-kafka-config]])
  (:import (java.util HashMap)
           (org.apache.kafka.clients.admin AdminClient OffsetSpec)
           (org.apache.kafka.common TopicPartition)))

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
  [fut]
  (deref fut 3000 nil))

(defn- sort-kvs
  [kvs]
  (sort-by first kvs))

(defn get-partitions
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
                    [topic (vec (sort-kvs partition-offsets))]))
             (into {}))))

(defn get-offsets-at
  "Gets the latest offsets for the given topic partitions at the specified point in time.

  'at' can either be :start, :end or an epoch millis long (representing an epoch time)."
  [^AdminClient kafka-admin-client topic at]
  (let [partitions  (get-partitions kafka-admin-client topic)
        offset-spec (case at
                      :start (OffsetSpec/earliest)
                      :end (OffsetSpec/latest)
                      (OffsetSpec/forTimestamp at))
        tp->os      (->> partitions
                         (map (fn [p] [(TopicPartition. topic p) offset-spec]))
                         (into {}))
        fut         (-> (.listOffsets kafka-admin-client tp->os)
                        (.all))]
    (some->> (wait-for-kafka-future fut)
             (map (fn [[tp lori]]
                    [(.partition tp) (.offset lori)]))
             (sort-kvs)
             (vec))))