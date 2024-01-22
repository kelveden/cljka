(ns cljka.kafka
  (:require [cljka.config :refer [normalize-kafka-config]])
  (:import (java.util HashMap)
           (org.apache.kafka.clients.admin AdminClient OffsetSpec)
           (org.apache.kafka.clients.consumer KafkaConsumer OffsetAndMetadata)
           (org.apache.kafka.common TopicPartition)))

; TODO: move to core namespace along with all other configuration merging logic
(defn ->topic-name
  [{:keys [topics]} topic]
  (if (keyword? topic)
    (get-in topics [topic :name])
    topic))

(defn ->admin-client
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

(defn- at->offset-spec
  [at]
  (case at
    :start (OffsetSpec/earliest)
    :end (OffsetSpec/latest)
    (OffsetSpec/forTimestamp at)))

(defn get-offsets-at
  "Gets the offsets for the given topic at the specified point in time.

  'at' can either be :start, :end or an epoch millis long (representing an epoch time)."
  [^AdminClient kafka-admin-client topic at]
  (let [partitions  (get-partitions kafka-admin-client topic)
        offset-spec (at->offset-spec at)
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

(defn get-offset-at
  "Gets the offset for the specified topic partition at the specified point in time.

  'at' can either be :start, :end or an epoch millis long (representing an epoch time)."
  [^AdminClient kafka-admin-client topic partition at]
  (let [offset-spec (at->offset-spec at)
        tp->os      {(TopicPartition. topic partition) offset-spec}
        fut         (-> (.listOffsets kafka-admin-client tp->os)
                        (.all))]
    (some->> (wait-for-kafka-future fut)
             (map (fn [[_ lori]]
                    (.offset lori)))
             (first))))

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
        {:total        total
         :by-partition by-partition})
      :no-lag-data)))

(defn set-group-offsets!
  "Sets the offset individually for each partition on the given topic to the offset indicated in the partition-offsets."
  [^AdminClient kafka-admin-client topic group-id partition-offsets]
  (let [tp->oams (->> partition-offsets
                      (map (fn [[p o]]
                             [(TopicPartition. topic p)
                              (OffsetAndMetadata. o)]))
                      (into {}))]
    (-> (.alterConsumerGroupOffsets kafka-admin-client group-id tp->oams)
        (.all)
        (wait-for-kafka-future))))

(defn set-group-offset!
  "Sets the group offset on a single partition to the specified value."
  [^AdminClient kafka-admin-client topic partition group-id offset]
  (let [tps->oam {(TopicPartition. topic partition) (OffsetAndMetadata. offset)}]
    (-> (.alterConsumerGroupOffsets kafka-admin-client group-id tps->oam)
        (.all)
        (wait-for-kafka-future))))

(defn get-topics
  "Gets a list of all topics."
  [^AdminClient kafka-admin-client]
  (-> (.listTopics kafka-admin-client)
      (.names)
      (wait-for-kafka-future)
      (sort)
      (vec)))

(defn start-consumer
  "Starts a new consumer on the specified topic from the specified point. The 'from'
  parameter can be any of :start, :end, a numeric offset. All partitions are consumed from
  the specified point. Alternatively, 'from' can be used to focus the consumer on specific partitions on the topic -
  in which case it will be a collection of partition/from pairs e.g. [[0 :start] [1 1412]]."
  [kafka-config topic from]
  (let [consumer (KafkaConsumer. ^HashMap (normalize-kafka-config kafka-config))
        tps      (if (coll? from)
                   (->> from
                        (map #(TopicPartition. topic (first %))))
                   (->> (.partitionsFor consumer topic)
                        (map #(TopicPartition. topic (.partition %)))))]
    (.assign consumer tps)

    (cond
      (coll? from)
      (doseq [[p p-from] from]
        (let [tp (TopicPartition. topic p)]
          (case p-from
            :start (.seekToBeginning consumer [tp])
            :end (.seekToEnd consumer [tp])
            (.seek consumer ^TopicPartition tp (long p-from)))))

      (number? from)
      (let [kafka-admin-client (->admin-client kafka-config)
            offsets            (get-offsets-at kafka-admin-client topic from)]
        (doseq [[p o] offsets]
          (.seek consumer (TopicPartition. topic p) ^long o)))

      (= :start from)
      (.seekToBeginning consumer tps)

      (= :end from)
      (.seekToEnd consumer tps))

    consumer))