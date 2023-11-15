(ns cljka.core-test
  (:require [clojure.test :refer :all]
            [cljka.core :as core]))

(deftest can-translate-keyword-topic-to-name
  (let [config {:environments {:my-env {:topics {:topic1 {:name "mytopic"}}}}}]
    (is (= "mytopic" (#'core/->topic-name config :my-env :topic1)))))

(deftest string-topic-is-not-translated
  (is (= "mytopic" (#'core/->topic-name {} :my-env "mytopic"))))