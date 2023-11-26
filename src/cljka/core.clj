(ns cljka.core
  (:require [cljka.kafka :as kafka]
            [cljka.confirm :refer [with-confirmation]]
            [cljka.config :refer [load-config ->kafka-config]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string]))

(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))
(s/def ::topic (s/or :keyword keyword?
                     :string ::non-blank-string))
(s/def ::consumer-group ::non-blank-string)
(s/def ::environment keyword?)

(s/def ::kafka-config (s/map-of ::non-blank-string ::non-blank-string))
(s/def ::partition-offsets (s/coll-of (s/cat :partition nat-int? :offset int?)))
(s/def ::at (s/or :start :start :end :end :timestamp int?))
(s/def ::offset (s/or :start :start :end :end :offset nat-int?))

(s/def ::by-partition ::partition-offsets)
(s/def ::total int?)

(defn- ->topic-name
  [config environment topic]
  (if (keyword? topic)
    (if-let [topic-name (some-> config :environments environment :topics topic :name)]
      topic-name
      (throw (RuntimeException. (format "Could not find a topic name matching %s" topic))))
    topic))

(defn- create-client
  [environment & [topic]]
  (let [config       (load-config)
        kafka-config (->kafka-config config environment topic)]
    {:client       (kafka/->admin-client kafka-config)
     :config       config
     :kafka-config kafka-config}))

(defn get-partitions
  "Gets a vector of partitions available for the given topic."
  [environment topic]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (kafka/get-partitions client topic-name)))

(s/fdef get-partitions
        :args (s/cat :environment ::environment
                     :topic ::topic)
        :ret (s/coll-of nat-int?))

(defn get-group-offsets
  "Gets the offsets for the specified consumer group on the given topic."
  [environment topic consumer-group]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (kafka/get-group-offsets client topic-name consumer-group)))

(s/fdef get-group-offsets
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :consumer-group ::consumer-group)
        :ret ::partition-offsets)

(defn get-offsets-at
  "Gets the offsets for the partitions of a topic at a particular point in time.

  at can be :start, :end or a number representing an epoch milli point in time."
  [environment topic at]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (kafka/get-offsets-at client topic-name at)))

(s/fdef get-offsets-at
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :at ::at)
        :ret ::partition-offsets)

(defn get-offset-at
  "Gets the offset for the specified partition of a topic at a particular point in time.

  at can be :start, :end or a number representing an epoch milli point in time."
  [environment topic partition at]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (kafka/get-offset-at client topic-name partition at)))

(s/fdef get-offset-at
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :partition nat-int?
                     :at ::at)
        :ret nat-int?)

(defn get-topics
  "Lists all topics in alphabetical order."
  [environment]
  (let [{:keys [client]} (create-client environment)]
    (kafka/get-topics client)))

(s/fdef get-topics
        :args (s/cat :environment ::environment)
        :ret (s/coll-of ::non-blank-string))

(defn get-lag
  "Gets the lag for the specified consumer group on the given topic."
  [environment topic consumer-group]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (kafka/get-lag client topic-name consumer-group)))

(s/fdef get-lag
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :consumer-group ::consumer-group)
        :ret (s/or :result (s/keys :req-un [::by-partition ::total])
                   :no-result #(= % :no-lag-data)))

(defn set-group-offsets!
  "Sets the group offset on all partitions to the specified value.

  partition-offsets is a vector of partition->offset pairs where offset is a numeric offset."
  [environment topic consumer-group partition-offsets]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (with-confirmation
      (format "Setting offsets for consumer group '%s' on topic '%s' to %s." consumer-group topic partition-offsets)
      (kafka/set-group-offsets! client topic-name consumer-group partition-offsets))))

(s/fdef set-group-offsets!
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :consumer-group ::consumer-group
                     :partition-offsets ::partition-offsets)
        :ret nil?)

(defn set-group-offset!
  "Sets the group offset on a single partition to the specified value.

  offset can either be :start, :end or a number representing a specific offset"
  [environment topic partition consumer-group offset]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (with-confirmation
      (format "Setting offsets for consumer group '%s' on partition '%s' topic '%s' to %s." consumer-group partition topic offset)
      (kafka/set-group-offset! client topic-name partition consumer-group offset))))

(s/fdef set-group-offset!
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :partition nat-int?
                     :consumer-group ::consumer-group
                     :offset ::offset)
        :ret nil?)

(stest/instrument)