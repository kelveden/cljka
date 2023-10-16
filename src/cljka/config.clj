(ns cljka.config
  (:require [clojure.edn :as edn]
            [clojure.walk :refer [stringify-keys]]))

{:environments {:env1 {:kafka      {:bootstrap.servers       ""
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
                                             :principal :user1}}}}}

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
  (-> (str (System/getProperty "user.home") "/.config/cljka/config.edn")
      (slurp)
      (edn/read-string)))

;
;(defmacro with
;  [something & body]
;  `(do
;     (def ~(symbol "thing") ~something)
;     ~@body))
;
;(with "whatever"
;      (prn thing))