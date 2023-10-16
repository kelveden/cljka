(ns cljka.test-utils
  (:require [clojure.test :refer :all]
            [clojure.string]
            [taoensso.timbre :as log]
            [cljka.config :refer [normalize-kafka-config]])
  (:import (java.util HashMap)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def ^:dynamic *kafka-config* {})

(defn with-kafka
  [f]
  (let [kafka             (doto (KafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:7.5.1"))
                            (.start))
        bootstrap-servers (-> (.getBootstrapServers kafka)
                              (clojure.string/replace "PLAINTEXT://" ""))
        kafka-config      {:bootstrap.servers bootstrap-servers}]
    (try
      (binding [*kafka-config* kafka-config]
        (log/reportf "==> Started kafka container with config %s." kafka-config)
        (f))
      (finally
        (.stop kafka)
        (log/reportf "==> Stopped kafka container.")))))

(defn ensure-topic!
  [topic partition-count]
  (let [config (normalize-kafka-config *kafka-config*)]
    (with-open [admin (AdminClient/create ^HashMap config)]
      (log/reportf "==> Ensuring topic %s exists." topic)
      (.createTopics admin [(NewTopic. topic partition-count (short 1))]))))