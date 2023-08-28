(ns cljka.channel-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [cljka.channel :as channel]))

(deftest closed?-returns-true-if-channel-closed
  (let [ch (async/chan)]
    (async/close! ch)
    (is (true? (channel/closed? ch)))))

(deftest closed?-returns-false-if-channel-open
  (let [ch (async/chan)]
    (is (false? (channel/closed? ch)))))