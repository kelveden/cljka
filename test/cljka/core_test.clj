(ns cljka.core-test
  (:require [clojure.test :refer :all]
            [cljka.core :refer [get-partitions]]
            [cljka.kafka :as kafka :refer [->admin-client]]
            [picomock.core :as pico]
            [cljka.config :refer [load-config]]
            [cljka.test-utils]))

(def ^:private dummy-admin-client-factory (fn [_] :admin-client))

(use-fixtures :once
              (fn [f]
                (with-redefs [->admin-client dummy-admin-client-factory]
                  (f))))

(deftest can-call-get-partitions-with-topic-key
  (with-redefs [load-config          (fn [] {:environments {:env1 {:topics {:topic1 {:name "mytopic"}}}}})
                kafka/get-partitions (pico/mock (fn [_ _]))]
    (get-partitions :env1 :topic1)

    (let [[arg1 arg2] (first (pico/mock-args kafka/get-partitions))]
      (is (= :admin-client arg1))
      (is (= "mytopic" arg2)))))

(deftest can-call-get-partitions-with-topic-string
  (with-redefs [load-config          (fn [] {})
                kafka/get-partitions (pico/mock (fn [_ _]))]
    (get-partitions :env1 "mytopic")

    (let [[arg1 arg2] (first (pico/mock-args kafka/get-partitions))]
      (is (= :admin-client arg1))
      (is (= "mytopic" arg2)))))

(deftest get-partitions-arguments-are-spec-checked
  (is (spec-error-thrown? #{[:topic :string] [:topic :keyword]}
                          (get-partitions :env1 1)))
  (is (spec-error-thrown? #{[:environment]}
                          (get-partitions "someenv" "sometopic"))))