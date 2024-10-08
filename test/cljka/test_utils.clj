(ns cljka.test-utils
  #_{:clj-kondo/ignore [:refer-all]}
  (:require [cljka.config :refer [normalize-kafka-config]]
            [cljka.kafka :as kafka]
            [clojure.string]
            [clojure.test :refer :all]
            [taoensso.timbre :as log])
  (:import (clojure.lang ExceptionInfo)
           (java.time Duration)
           (java.util HashMap UUID)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)
           (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def ^:dynamic *kafka-config* {})
(def ^:dynamic *kafka-admin-client* nil)

;
;--- Assertion helpers
;

(defmacro is-eventually?
  [& body]
  `(do (let [fut# (future (while (not ~@body)
                            (Thread/sleep 100)))]
         (deref fut# 1000 nil))
       (is ~@body)))

(defmacro is-never?
  [& body]
  `(do (let [fut# (future (while (not ~@body)
                            (Thread/sleep 100)))]
         (deref fut# 1000 nil))
       (is (not ~@body))))

; Assert that a specific a Clojure spec error with the specified paths and function is thrown
(defmethod assert-expr 'spec-error-thrown? [msg form]
  (let [expected-paths (nth form 1)
        body           (nthnext form 2)]
    `(try ~@body
          (do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch ExceptionInfo e#
            (let [actual-paths# (->> (ex-data e#)
                                     :clojure.spec.alpha/problems
                                     (map :path)
                                     (set))]
              (if (= ~expected-paths actual-paths#)
                (do-report {:type     :pass, :message ~msg,
                            :expected '~form, :actual e#})
                (do-report {:type     :fail, :message ~msg,
                            :expected '~form, :actual {:paths actual-paths#
                                                       :e     e#}})))
            e#))))

;
;--- Kafka helpers
;

(defn with-kafka
  [f]
  (let [kafka             (doto (KafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:7.5.3"))
                            (.start))
        bootstrap-servers (-> (.getBootstrapServers kafka)
                              (clojure.string/replace "PLAINTEXT://" ""))
        kafka-config      {:bootstrap.servers bootstrap-servers}]
    (with-open [kafka-admin-client (kafka/->admin-client kafka-config)]
      (try
        (binding [*kafka-config*       kafka-config
                  *kafka-admin-client* kafka-admin-client]
          (log/reportf "==> Started kafka container with config %s." kafka-config)
          (f))
        (finally
          (.stop kafka)
          (log/reportf "==> Stopped kafka container."))))))

(defn with-consumer
  ([key-deserializer value-deserializer consumer-group f]
   (let [config (-> *kafka-config*
                    (merge {:key.deserializer   key-deserializer
                            :value.deserializer value-deserializer
                            :group.id           consumer-group})
                    (normalize-kafka-config))]
     (log/reportf "==> Starting a consumer with config %s." config)
     (let [consumer (KafkaConsumer. ^HashMap config)]
       (try
         (f consumer)
         (finally
           (.close consumer (Duration/ofSeconds 0))
           (log/reportf "==> Closed consumer with config %s." config))))))
  ([consumer-group f]
   (with-consumer StringDeserializer StringDeserializer consumer-group f))
  ([f]
   (with-consumer StringDeserializer StringDeserializer (str (UUID/randomUUID)) f)))

(defn with-producer
  ([kafka-config f]
   (let [config (normalize-kafka-config kafka-config)]
     (log/reportf "==> Starting a producer with config %s." config)
     (let [producer (KafkaProducer. ^HashMap config)]
       (try
         (f producer)
         (finally
           (.close producer (Duration/ofSeconds 0))
           (log/reportf "==> Closed producer with config %s." config))))))

  ([key-serializer value-serializer f]
   (with-producer (-> *kafka-config*
                      (merge {:key.serializer   key-serializer
                              :value.serializer value-serializer})) f))

  ([f]
   (with-producer StringSerializer StringSerializer f)))

(defn produce!
  [producer topic kvs]
  (doseq [[k v] kvs]
    (let [r (ProducerRecord. topic k v)
          f (.send producer r)]
      (deref f 3000 nil))))

(defn ensure-topic!
  [topic partition-count]
  (let [config (normalize-kafka-config *kafka-config*)]
    (with-open [admin (AdminClient/create ^HashMap config)]
      (log/reportf "==> Ensuring topic %s exists." topic)
      (.createTopics admin [(NewTopic. ^String topic ^int partition-count ^short (short 1))]))))
