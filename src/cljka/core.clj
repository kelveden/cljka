(ns cljka.core
  (:require [cljka.kafka :as kafka]
            [cljka.config :refer [load-config ->kafka-config ->topic-name]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string]))

(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))
(s/def ::topic (s/or :keyword keyword?
                     :string ::non-blank-string))
(s/def ::environment keyword?)

(s/def ::kafka-config (s/map-of ::non-blank-string ::non-blank-string))

(defn get-partitions
  "Gets a vector of partitions available for the given topic."
  [environment topic]
  (let [config       (load-config)
        kafka-config (->kafka-config config environment topic)]
    (kafka/get-partitions
      (kafka/->admin-client kafka-config)
      (->topic-name config environment topic))))

(s/fdef get-partitions
        :args (s/cat :environment ::environment
                     :topic ::topic)
        :ret (s/coll-of nat-int?))

(stest/instrument)