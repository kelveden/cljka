(ns cljka.kafka
  (:require [cljka.config :refer [normalize-kafka-config]])
  (:import (java.util HashMap)
           (org.apache.kafka.clients.admin AdminClient OffsetSpec)
           (org.apache.kafka.clients.consumer OffsetAndMetadata)
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
  "Gets the offsets of the given consumer group on the specified topic."
  [^AdminClient kafka-admin-client topic group-id]
  (let [fut (-> (.listConsumerGroupOffsets kafka-admin-client ^String group-id)
                (.partitionsToOffsetAndMetadata))]
    (some->> (wait-for-kafka-future fut)
             ; Convert to map of topic->vec of partition/offset pairs
             (reduce (fn [acc [topic-partition offset-metadata]]
                       (let [t (.topic topic-partition)
                             p (.partition topic-partition)
                             o (.offset offset-metadata)]
                         (cond-> acc
                                 (= topic t) (conj [p o]))))
                     [])
             (vec)
             (sort-kvs))))

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

(defn get-lag
  "Gets the lag for the specified consumer group on the specified topic."
  [^AdminClient kafka-admin-client topic group-id]
  (let [group-offsets  (get-group-offsets kafka-admin-client topic group-id)
        latest-offsets (get-offsets-at kafka-admin-client topic :end)]
    (if (not-empty group-offsets)
      (let [by-partition (-> (map (fn [[partition group-offset] [_ latest-offset]]
                                    [partition (- latest-offset group-offset)])
                                  group-offsets latest-offsets)
                             (vec))
            total        (->> by-partition (map second) (reduce +))]
        {:total      total
         :partitions by-partition})
      :no-lag-data)))

(defn set-group-offsets!
  "Sets the group offset on all partitions to the specified value.

  offset can either be :start, :end or a number representing a specific offset"
  [^AdminClient kafka-admin-client topic group-id offset]
  (let [topic-offsets (if (keyword? offset)
                        (get-offsets-at kafka-admin-client topic offset)
                        (->> (get-partitions kafka-admin-client topic)
                             (map #(vector % offset))))
        tps->oam      (->> topic-offsets
                           (map (fn [[p o]]
                                  [(TopicPartition. topic p)
                                   (OffsetAndMetadata. o)]))
                           (into {}))]
    (.alterConsumerGroupOffsets kafka-admin-client group-id tps->oam)))

(defn get-topics
  "Gets a list o all topics."
  [^AdminClient kafka-admin-client]
  (-> (.listTopics kafka-admin-client)
      (.names)
      (wait-for-kafka-future)
      (sort)
      (vec)))