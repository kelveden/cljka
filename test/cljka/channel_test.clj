(ns cljka.channel-test
  (:require [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.test :refer :all]
            [clojure.core.async :as async]
            [cljka.channel :as channel]))

(deftest closed?-returns-true-if-channel-closed
  (let [ch (async/chan)]
    (async/close! ch)
    (is (true? (channel/closed? ch)))))

(deftest closed?-returns-false-if-channel-open
  (let [ch (async/chan)]
    (is (false? (channel/closed? ch)))))

(deftest can-send-channel-to-atom-sink
  ; GIVEN an input channel
  (let [ch   (async/chan 10)
        data (vec (range 0 9))
        a    (atom [])
        sink (channel/sink a)]
    ; AND an atom sink
    (channel/to! ch [sink])

    ; WHEN data arrives in the input channel
    (doseq [x data]
      (async/>!! ch x))

    (let [fut (future (while (not= data @a)
                        (Thread/sleep 100)))]
      (deref fut 3000 nil))

    ; THEN eventually the same data arrives at the sink
    (is (= data @a))))

(deftest sink-channels-close-once-input-channel-closes
  ; GIVEN an input channel
  (let [ch    (async/chan)
        a1    (atom [])
        a2    (atom [])
        sink1 (channel/sink a1)
        sink2 (channel/sink a2)]
    ; AND sinks
    (channel/to! ch [sink1 sink2])

    (is (false? (async-protocols/closed? sink1)))
    (is (false? (async-protocols/closed? sink2)))

    ; WHEN the input channel closes
    (async/close! ch)

    (let [fut (future (while (or (not (async-protocols/closed? sink1))
                                 (not (async-protocols/closed? sink2)))
                        (Thread/sleep 100)))]
      (deref fut 1000 nil))

    ; THEN the sinks are eventually closed
    (is (true? (async-protocols/closed? sink1)))
    (is (true? (async-protocols/closed? sink2)))))

(deftest can-stop-sink-channel-closing-once-input-channel-closes
  ; GIVEN an input channel
  (let [ch   (async/chan)
        a    (atom [])
        sink (channel/sink a)]
    ; AND a sink with an explicit instruction NOT to close it
    (channel/to! ch [sink] {:close-sinks? false})

    (is (false? (async-protocols/closed? sink)))

    ; WHEN the input channel closes
    (async/close! ch)

    (let [fut (future (while (not (async-protocols/closed? sink))
                        (Thread/sleep 100)))]
      (deref fut 1000 nil))

    ; THEN the sink channel does not close
    (is (false? (async-protocols/closed? sink)))))