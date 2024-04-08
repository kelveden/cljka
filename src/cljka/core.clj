(ns cljka.core
  (:require [cheshire.core :as json]
            [cljka.kafka :as kafka]
            [cljka.confirm :refer [with-confirmation]]
            [cljka.config :refer [->kafka-config ->topic-config ->deserialization-config]]
            [cljka.channel :as channel]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string]
            [taoensso.timbre :as log]
            [tick.core :as t])
  (:import (java.time Duration)
           (java.util UUID)
           (org.apache.kafka.clients.consumer Consumer)))
(def ^:no-doc config (atom nil))

(s/check-asserts true)

(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))
(s/def ::topic (s/or :keyword keyword?
                     :string ::non-blank-string))
(s/def ::consumer-group ::non-blank-string)
(s/def ::environment keyword?)

(s/def ::partition nat-int?)
(s/def ::offset int?)
(s/def ::timestamp nat-int?)

(s/def ::partition-offsets (s/coll-of (s/cat :partition ::partition :offset ::offset)))
(s/def ::offset-at (s/or :keyword #{:start :end} :timestamp ::timestamp))
(s/def ::offset-definition (s/or :keyword #{:start :end} :offset ::offset))

(s/def ::by-partition ::partition-offsets)
(s/def ::total int?)

(defn set-config!
  [new-config]
  (reset! config new-config))

(defn- ->topic-name
  [config environment topic]
  (if (keyword? topic)
    (if-let [topic-name (some-> config
                                (->topic-config environment topic)
                                :name)]
      topic-name
      (throw (RuntimeException. (format "Could not find a topic name matching %s" topic))))
    topic))

(defn- create-client
  [environment & [topic]]
  (let [kafka-config (->kafka-config @config environment topic)]
    {:client       (kafka/->admin-client kafka-config)
     :config       @config
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

  `at` can either be :start, :end or an epoch millis long (representing an epoch time). If epoch millis are specified
  then the offset for each partition will be the first one with a timestamp after the specified point in time - -1 will
  be returned if such offset can be found."
  [environment topic at]
  (let [{:keys [client config]}
        (create-client environment topic)

        topic-name
        (->topic-name config environment topic)]
    (kafka/get-offsets-at client topic-name at)))

(s/fdef get-offsets-at
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :at ::offset-at)
        :ret ::partition-offsets)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn get-offset-at
  "Gets the offset for the specified partition of a topic at a particular point in time.

  `at` can either be :start, :end or an epoch millis long (representing an epoch time). If epoch millis are specified
  then the offset will be the first one with a timestamp after the specified point in time - -1 will
  be returned if such offset can be found."
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
                     :at ::offset-at)
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

  `partition-offsets` is a vector of partition->offset pairs where each offset is a numeric offset."
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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn set-group-offset!
  "Sets the group offset on a single partition to the specified value.

  `offset` can either be `:start`, `:end` or a number representing a specific offset"
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
                     :offset ::offset-definition)
        :ret nil?)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn get-kafka-config
  "Returns the configuration map that will be used in cljka operations for the specified environment and topic. Useful
  for diagnosing problems."
  [environment topic]
  (->kafka-config @config environment topic))

(s/fdef get-kafka-config
        :args (s/cat :environment ::environment
                     :topic ::topic)
        :ret nil?)

(defn consume!
  "Starts a new consumer on the specified topic from the specified point. The 'from'
  parameter can be any of :start, :end, a numeric offset. All partitions are consumed from
  the specified point. Alternatively, 'from' can be used to focus the consumer on specific partitions on the topic -
  in which case it will be a collection of partition/from pairs e.g. [[0 :start] [1 1412]]."
  [environment topic from]
  (let [consumer-group         (str "cljka-" (UUID/randomUUID))
        kafka-config           (-> (->kafka-config @config environment topic)
                                   (assoc "group.id" consumer-group))
        deserialization-config (->deserialization-config @config environment topic)
        topic-name             (->topic-name @config environment topic)
        consumer               (kafka/start-consumer kafka-config topic-name from)
        json?                  (some-> deserialization-config :json?)
        cr->message            (fn [cr] {:key            (.key cr)
                                         :partition      (.partition cr)
                                         :offset         (.offset cr)
                                         :timestamp      (-> (.timestamp cr) t/instant)
                                         :timestamp-type (str (.timestampType cr))
                                         :value          (cond-> (.value cr)
                                                                 json? (-> (str)
                                                                           (json/parse-string true)))
                                         :type           (type (.value cr))})
        ch                     (async/chan)]
    (prn (str "Consumer group: " consumer-group))
    (future
      (try
        (loop []
          (let [messages (->> (.poll ^Consumer consumer (Duration/ofMillis 1000))
                              (map cr->message))]
            ; Push messages to the channel
            (doseq [msg messages]
              (when-not (async-protocols/closed? ch)
                (async/>!! ch msg)))

            ; Get the next batch of messages
            (when-not (async-protocols/closed? ch)
              (recur))))

        (catch Throwable e
          (log/error e))

        (finally
          (.close consumer)
          (async/close! ch)
          (log/report "Consumer closed."))))
    ch))

(s/fdef consume!
        :args (s/cat :environment ::environment
                     :topic ::topic
                     :from (s/or :offset ::offset-definition
                                 :partition-offsets (s/coll-of (s/cat :partition ::partition :offset ::offset-definition))))
        :ret ::channel/channel)

(stest/instrument)