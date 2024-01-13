(ns cljka.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.walk :refer [stringify-keys]]
            [clojure.string]
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]))

(def ^:dynamic *principal* nil)

(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))

;
; environment
;

; kafka
(s/def :kafka/bootstrap.servers ::non-blank-string)
(s/def ::kafka (s/and (s/keys :req-un [:kafka/bootstrap.servers])
                      (s/map-of keyword? string?)))

; schema registry
(s/def :schema-registry/base-url ::non-blank-string)
(s/def :schema-registry/username ::non-blank-string)
(s/def :schema-registry/password ::non-blank-string)
(s/def ::schema-registry (s/keys :req-un [:schema-registry/base-url]
                                 :opt-un [:schema-registry/username :schema-registry/password]))

; principals
(s/def :principal/kafka (s/map-of keyword? string?))
(s/def :principal/schema-registry (s/keys :req-un [:schema-registry/username :schema-registry/password]))
(s/def ::principal (s/keys :opt-un [:principal/kafka :principal/schema-registry]))
(s/def ::principals (s/map-of keyword? ::principal))

; topics
(s/def :topic/principal keyword?)
(s/def :topic/name ::non-blank-string)
(s/def ::topics (s/map-of keyword? (s/keys :req-un [:topic/name]
                                           :opt-un [:topic/principal])))

(s/def ::environment (s/keys :req-un [::kafka]
                             :opt-un [::principals ::topics]))

;
; config
;

(s/def ::environments (s/map-of keyword? ::environment))
(s/def ::config (s/keys :req-un [::environments]))

(def x {:environments {:env1 {:kafka      {:bootstrap.servers       "dfsdfd"
                                           :security.protocol       "ssl"
                                           :ssl.key.password        ""
                                           :ssl.keystore.password   ""
                                           :ssl.keystore.type       "pkcs12"
                                           :ssl.truststore.password "password"}
                              :principals {:user1 {:kafka           {:ssl.keystore.location   ""
                                                                     :ssl.truststore.location ""}
                                                   :schema-registry {:username ""
                                                                     :password ""}}}
                              :topics     {:topic1 {:name      "some_topic1"
                                                    :principal :user1}
                                           :topic2 {:name      "some_topic2"
                                                    :principal :user1}}}}})

{:environments {:env1 {:kafka  {:bootstrap.servers "localhost:21556"}
                       :topics {:topic1 {:name "topic1"}
                                :topic2 {:name "topic2"}}}}}

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
      (->> (slurp config-file-path)
           (edn/read-string)
           (s/assert ::config))
      {})))

(defn ->kafka-config
  "Selects and merges kafka config based on either a) a principal (if one has been selected with with-principal); b) a topic
  (based on the principal assigned to that topic in the configuration); or c) just the core kafka config if (a) and (b) fail."
  [config environment topic]
  (let [core-kafka-config      (-> config :environments environment :kafka)
        principal              (cond
                                 (some? *principal*) *principal*
                                 (keyword? topic) (-> config :environments environment :topics topic :principal))
        principal-kafka-config (some-> config :environments environment :principals principal :kafka)]
    (->> (merge core-kafka-config principal-kafka-config)
         (normalize-kafka-config))))

(defmacro with-principal
  [principal & body]
  `(binding [*principal* ~principal]
     ~@body))
