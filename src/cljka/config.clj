(ns cljka.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.walk :refer [stringify-keys]]
            [clojure.string]
            [taoensso.timbre :as log]))

(def ^:dynamic *principal* nil)

(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))

;
; environment
;

; kafka
(s/def :kafka/bootstrap.servers ::non-blank-string)
(s/def ::kafka (s/and (s/keys :req-un [:kafka/bootstrap.servers])
                      (s/map-of keyword? string?)))

; principals
(s/def :principal/kafka (s/map-of keyword? string?))
(s/def ::principal (s/keys :opt-un [:principal/kafka]))
(s/def ::principals (s/map-of keyword? ::principal))

; topics
(s/def :topic/principal keyword?)
(s/def :topic/name ::non-blank-string)
(s/def ::topics (s/map-of keyword? (s/keys :opt-un [:topic/name :topic/principal])))

(s/def ::environment (s/keys :req-un [::kafka]
                             :opt-un [::principals ::topics]))

;
; config
;

(s/def ::environments (s/map-of keyword? ::environment))
(s/def ::config (s/keys :req-un [::environments]
                        :opt-un [::topics ::principals]))

(defn normalize-kafka-config
  [kafka-config]
  (->> kafka-config
       (stringify-keys)
       (map (fn [[k v]] [k (if (number? v) (int v) v)]))
       (into {})))

(defn load-config
  "Reloads the cljka configuration file."
  []
  (let [config-file-path (str (System/getProperty "user.home") "/.config/cljka/config.edn")]
    (log/info "Loading configuration from file..." {:file config-file-path})
    (if (.exists (io/file config-file-path))
      (let [config (->> (slurp config-file-path)
                        (edn/read-string))]
        (when-not (s/valid? ::config config)
          (println "@@@@@@ INVALID CONFIGURATION! @@@@@@")
          (s/explain ::config config))
        config)
      {})))

(defn- ->environment-kafka-config
  [config environment]
  (if-let [environment-config (-> config :environments environment :kafka)]
    environment-config
    (throw (ex-info "Environment could not be found." {:environment environment}))))

(defn- ->principal-kafka-config
  [config environment principal]
  (if-let [principal-config (merge (-> config :principals principal :kafka)
                                   (-> config :environments environment :principals principal :kafka))]
    principal-config
    (throw (ex-info "Principal could not be found." {:principal principal}))))

(defn- ->topic-config
  [config environment topic]
  (when (keyword? topic)
    (if-let [topic-config (merge (-> config :topics topic)
                                 (-> config :environments environment :topics topic))]
      topic-config
      (throw (ex-info "Topic could not be found." {:topic topic})))))

(defn ->kafka-config
  "Selects and merges kafka config based on either a) a principal (if one has been selected with with-principal); b) a topic
  (based on the principal assigned to that topic in the configuration); or c) just the core kafka config if (a) and (b) fail."
  [config environment topic]
  (let [environment-kafka-config (->environment-kafka-config config environment)
        topic-config             (->topic-config config environment topic)
        principal                (cond
                                   (some? *principal*) *principal*
                                   (some? topic-config) (:principal topic-config))
        principal-kafka-config   (some->> principal (->principal-kafka-config config environment))]
    (->> (merge environment-kafka-config principal-kafka-config)
         (normalize-kafka-config))))

(defmacro with-principal
  [principal & body]
  `(binding [*principal* ~principal]
     ~@body))
