(ns cljka.config-test
  #_{:clj-kondo/ignore [:refer-all]}
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [cljka.config :refer [->kafka-config with-principal ->deserialization-config] :as config])
  (:import (clojure.lang ExceptionInfo)))

(deftest sample-config-is-valid
  (let [config {:environments {:env1 {:kafka      {:bootstrap.servers       "dfsdfd"
                                                   :security.protocol       "ssl"
                                                   :ssl.key.password        ""
                                                   :ssl.keystore.password   ""
                                                   :ssl.keystore.type       "pkcs12"
                                                   :ssl.truststore.password "password"}
                                      :principals {:user1 {:kafka {:ssl.keystore.location   ""
                                                                   :ssl.truststore.location ""}}}
                                      :topics     {:topic1 {:principal :user1}
                                                   :topic2 {:principal :user1}}}}
                :topics       {:topic1 {:name "some_topic1"}
                               :topic2 {:name "some_topic2"}}}]
    (s/explain ::config/config config)
    (is (s/valid? ::config/config config))))

(deftest environment-kafka-config-is-selected
  (let [config {:environments {:my-env {:kafka {:core-field1 "core-value1"
                                                :core-field2 "core-value2"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"}
           (->kafka-config config :my-env "sometopic")))))

(deftest missing-environment-causes-error
  (let [config {:environments {}}]
    (is (thrown-with-msg? ExceptionInfo #"Environment could not be found."
                          (->kafka-config config :my-env "sometopic")))))

(deftest missing-principal-causes-error
  (let [config {:environments {:my-env {:kafka {:core-field1 "core-value1"
                                                :core-field2 "core-value2"}}}}]
    (is (thrown-with-msg? ExceptionInfo #"Principal could not be found."
                          (with-principal
                            :my-user
                            (->kafka-config config :my-env "sometopic"))))))

(deftest missing-topic-causes-error
  (let [config {:environments {:my-env {:kafka {:core-field1 "core-value1"
                                                :core-field2 "core-value2"}}}}]
    (is (thrown-with-msg? ExceptionInfo #"Topic could not be found."
                          (->kafka-config config :my-env :sometopic)))))

(deftest root-principal-kafka-config-is-merged
  (let [config {:environments {:my-env {:kafka {:core-field1 "core-value1"
                                                :core-field2 "core-value2"}}}
                :principals   {:my-user {:kafka {:user-field1 "value1"
                                                 :user-field2 "value2"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value1"
            "user-field2" "value2"}
           (with-principal
             :my-user
             (->kafka-config config :my-env "sometopic"))))))

(deftest environment-principal-kafka-config-is-merged-with-root-principal-kafka-config
  (let [config {:environments {:my-env {:kafka      {:core-field1 "core-value1"
                                                     :core-field2 "core-value2"}
                                        :principals {:my-user {:kafka {:user-field1 "env-value1"}}}}}
                :principals   {:my-user {:kafka {:user-field1 "root-value1"
                                                 :user-field2 "root-value2"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "env-value1"
            "user-field2" "root-value2"}
           (with-principal
             :my-user
             (->kafka-config config :my-env "sometopic"))))))

(deftest principal-referenced-by-topic-kafka-config-is-merged
  (let [config {:environments {:my-env {:kafka  {:core-field1 "core-value1"
                                                 :core-field2 "core-value2"}
                                        :topics {:topic1 {:principal :my-user}}}}
                :principals   {:my-user {:kafka {:user-field1 "value1"
                                                 :user-field2 "value2"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value1"
            "user-field2" "value2"}
           (->kafka-config config :my-env :topic1)))))

(deftest principal-referenced-by-topic-is-overidden-by-with-principal
  (let [config {:environments {:my-env {:kafka  {:core-field1 "core-value1"
                                                 :core-field2 "core-value2"}
                                        :topics {:topic1 {:principal :my-user}}}}
                :principals   {:my-user      {:kafka {:user-field1 "value1"
                                                      :user-field2 "value2"}}
                               :another-user {:kafka {:user-field1 "value3"
                                                      :user-field2 "value4"}}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value3"
            "user-field2" "value4"}
           (with-principal
             :another-user
             (->kafka-config config :my-env :topic1))))))

(deftest environment-topic-config-is-merged-with-environment-topic-config
  (let [config {:environments {:my-env {:kafka  {:core-field1 "core-value1"
                                                 :core-field2 "core-value2"}
                                        :topics {:topic1 {:name "sometopic-my-env"}}}}
                :principals   {:my-user {:kafka {:user-field1 "value1"}}}
                :topics       {:topic1 {:principal :my-user}}}]
    (is (= {"core-field1" "core-value1"
            "core-field2" "core-value2"
            "user-field1" "value1"}
           (->kafka-config config :my-env :topic1)))))

(deftest deserialization-config-is-merged-by-root-environment-topic
  (let [config {:deserialization {:prop1 :a
                                  :prop2 :b
                                  :prop3 :c}
                :environments    {:my-env {:deserialization {:prop2 :d
                                                             :prop3 :e}}}
                :topics          {:topic1 {:deserialization {:prop3 :f
                                                             :prop4 :g}}}}]
    (is (= {:prop1 :a :prop2 :d :prop3 :f :prop4 :g}
           (->deserialization-config config :my-env :topic1)))))