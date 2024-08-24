(ns cljka.core-test
  #_{:clj-kondo/ignore [:refer-all]}
  (:require [cljka.channel :as ch]
            [cljka.core :as core]
            [cljka.test-utils :refer [*kafka-config* ensure-topic! is-eventually? produce! with-kafka with-producer]]
            [clojure.test :refer :all]
            [taoensso.timbre :as log])
  (:import (java.util UUID)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)))

(log/set-config! (-> log/default-config
                     (merge {:min-level :warn})))

(use-fixtures :once with-kafka)

(defn- generate-random-messages
  [n]
  (repeatedly n #(vector (str (UUID/randomUUID))
                         (str (UUID/randomUUID)))))

(deftest can-translate-keyword-topic-to-name
  (let [config {:environments {:my-env {:topics {:topic1 {:name "mytopic"}}}}}]
    (is (= "mytopic" (#'core/->topic-name config :my-env :topic1)))))

(deftest string-topic-is-not-translated
  (is (= "mytopic" (#'core/->topic-name {} :my-env "mytopic"))))

(deftest can-consume
  (let [config   {:environments
                  {:test
                   {:kafka (assoc *kafka-config*
                                  :key.deserializer StringDeserializer
                                  :value.deserializer StringDeserializer)}}}
        topic    (str (UUID/randomUUID))
        messages (generate-random-messages 10)]
    (core/set-config! config)
    (ensure-topic! topic 2)

    ; AND the messages are produced to the topic
    (with-producer StringSerializer StringSerializer
      (fn [producer]
        (produce! producer topic messages)))

    ; WHEN consumption is started at the beginning of the topic.
    (let [c (core/consume! :test topic :start)
          a (atom [])]
      (ch/to! c a)
      (try
        ; THEN all messages are consumed
        (is-eventually? (= 10 (count @a)))

        (is (= (set messages)
               (set (->> @a
                         (map #(vector (:key %) (:value %)))
                         (vec)))))
        (finally
          (ch/close! c))))))
