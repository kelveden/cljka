(ns cljka.kafka
  (:require [cljka.config :refer [normalize-kafka-config]]
            [taoensso.timbre :as log])
  (:import (java.time Duration)
           (java.util HashMap)
           (org.apache.kafka.clients.consumer KafkaConsumer)))

(defn- ->topic-name
  [{:keys [topics]} topic]
  (if (keyword? topic)
    (get-in topics [topic :name])
    topic))

(defn- new-consumer
  [kafka-config]
  (let [config (-> kafka-config
                   (normalize-kafka-config))]
    (log/debugf "==> Starting a consumer with config %s." config)
    (KafkaConsumer. ^HashMap config)))

(defn get-topic-partitions
  "Gets a vector of partitions available for the given topic."
  [env-config topic]
  (if-let [topic-name (->topic-name env-config topic)]
    (let [consumer (-> env-config :kafka (new-consumer))]
      (try
        (->> (.partitionsFor consumer topic-name)
             (map #(.partition %))
             (sort)
             (vec))
        (finally
          (.close consumer (Duration/ofSeconds 0)))))
    :topic-not-configured))
