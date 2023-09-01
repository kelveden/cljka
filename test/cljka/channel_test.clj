(ns cljka.channel-test
  (:require [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [clojure.core.async :as async]
            [cljka.channel :as channel]
            [clojure.string :refer [join]])
  (:import (java.io File StringWriter)
           (java.util UUID)))

(defmacro is-eventually?
  [& body]
  `(do (let [fut# (future (while (not ~@body)
                            (Thread/sleep 100)))]
         (deref fut# 1000 nil))
       (is ~@body)))

(defmacro is-never?
  [& body]
  `(do (let [fut# (future (while (not ~@body)
                            (Thread/sleep 100)))]
         (deref fut# 1000 nil))
       (is (not ~@body))))

(deftest closed?-returns-true-if-channel-closed
  (let [ch (async/chan)]
    (async/close! ch)
    (is (true? (channel/closed? ch)))))

(deftest closed?-returns-false-if-channel-open
  (let [ch (async/chan)]
    (is (false? (channel/closed? ch)))))

(deftest can-send-channel-to-atom-sink
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        data     (vec (range 0 9))
        a        (atom [])
        sink     (channel/sink a)]
    ; AND an atom sink
    (channel/to! input-ch [sink])

    ; WHEN data arrives in the input channel
    (doseq [x data]
      (async/>!! input-ch x))

    ; THEN eventually the same data arrives at the sink
    (is-eventually? (= data @a))
    ; AND the sink is kept open for more data
    (is (not (async-protocols/closed? sink)))))

(deftest sink-channels-close-once-input-channel-closes
  ; GIVEN an input channel
  (let [input-ch (async/chan)
        a1       (atom [])
        a2       (atom [])
        sink1    (channel/sink a1)
        sink2    (channel/sink a2)]
    ; AND sinks
    (channel/to! input-ch [sink1 sink2])

    (is (false? (async-protocols/closed? sink1)))
    (is (false? (async-protocols/closed? sink2)))

    ; WHEN the input channel closes
    (async/close! input-ch)

    ; THEN the sinks are eventually closed
    (is-eventually? (async-protocols/closed? sink1))
    (is-eventually? (async-protocols/closed? sink2))))

(deftest can-stop-sink-channel-closing-once-input-channel-closes
  ; GIVEN an input channel
  (let [input-ch (async/chan)
        a        (atom [])
        sink     (channel/sink a)]
    ; AND a sink with an explicit instruction NOT to close it
    (channel/to! input-ch [sink] {:close-sinks? false})

    (is (false? (async-protocols/closed? sink)))

    ; WHEN the input channel closes
    (async/close! input-ch)

    ; THEN the sink channel does not close
    (is-never? (async-protocols/closed? sink))))

(deftest only-n-items-are-sent-to-sink
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        a        (atom [])
        sink     (channel/sink a {:n 3})]
    ; AND an atom sink with a maximum of 3 items
    (channel/to! input-ch [sink])

    ; WHEN fewer than N items arrives in the input channel
    (doseq [x [0 1]]
      (async/>!! input-ch x))

    ; THEN eventually the <n items arrive at the sink
    (is-eventually? (= [0 1] @a))
    ; AND the sink is kept open as N has not been reached yet
    (is (not (async-protocols/closed? sink)))

    ; AND WHEN more items (to bring the total to >N) arrive at the sink
    (doseq [x [2 3]]
      (async/>!! input-ch x))

    ; THEN the data arrives at the sink up to N items
    (is-eventually? (= [0 1 2] @a))
    ; AND the sink is immediately closed
    (is (async-protocols/closed? sink))))

(deftest only-items-matching-predicate-are-sent-to-sink
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        data     (vec (range 0 9))
        a        (atom [])
        sink     (channel/sink a {:pred odd?})]
    ; AND an atom sink with a maximum of 3 items
    (channel/to! input-ch [sink])

    ; WHEN data arrives in the input channel
    (doseq [x data]
      (async/>!! input-ch x))

    ; THEN eventually the n items arrive at the sink
    (is-eventually? (= [1 3 5 7] @a))
    ; AND the sink is kept open
    (is (not (async-protocols/closed? sink)))))

(deftest only-n-items-matching-predicate-are-sent-to-sink
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        data     (vec (range 0 9))
        a        (atom [])
        sink     (channel/sink a {:pred odd? :n 2})]
    ; AND an atom sink with a maximum of 3 items
    (channel/to! input-ch [sink])

    ; WHEN data arrives in the input channel
    (doseq [x data]
      (async/>!! input-ch x))

    ; THEN eventually the n items arrive at the sink
    (is-eventually? (= [1 3] @a))
    ; AND the sink is closed (as n has been specified)
    (is (async-protocols/closed? sink))))

(deftest can-send-channel-to-file-sink
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        data     (vec (range 0 9))
        file     (File/createTempFile (str (UUID/randomUUID)) ".txt")
        sink     (channel/sink file)]
    ; AND a file sink
    (channel/to! input-ch [sink])

    ; WHEN data arrives in the input channel
    (doseq [x data]
      (async/>!! input-ch x))

    ; THEN eventually the same data arrives at the sink
    (is-eventually? (= (str (join "\n" data) "\n")
                       (slurp file)))
    ; AND the sink is kept open for more data
    (is (not (async-protocols/closed? sink)))))

(deftest can-send-channel-to-filepath-sink
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        data     (vec (range 0 9))
        path     (.getAbsolutePath (File/createTempFile (str (UUID/randomUUID)) ".txt"))
        sink     (channel/sink path)]
    ; AND a file sink
    (channel/to! input-ch [sink])

    ; WHEN data arrives in the input channel
    (doseq [x data]
      (async/>!! input-ch x))

    ; THEN eventually the same data arrives at the sink
    (is-eventually? (= (str (join "\n" data) "\n")
                       (slurp path)))
    ; AND the sink is kept open for more data
    (is (not (async-protocols/closed? sink)))))

(deftest can-send-channel-to-writer-sink
  ; GIVEN a string writer
  (with-open [sw (StringWriter.)]
    ; AND an input channel
    (let [input-ch (async/chan 10)
          data     [0 1 2 3]
          sink     (channel/sink sw {:printer (fn [s w]
                                                (.write w (format "{%s}" s)))})]
      ; AND a file sink
      (channel/to! input-ch [sink])

      ; WHEN data arrives in the input channel
      (doseq [x data]
        (async/>!! input-ch x))

      ; THEN eventually the same data arrives at the string writer
      (is-eventually? (= "{0}{1}{2}{3}" (str sw)))
      ; AND the sink is kept open for more data
      (is (not (async-protocols/closed? sink))))))

(deftest file-sink-options-are-passed-to-writer
  ; GIVEN an input channel
  (let [input-ch (async/chan 10)
        file     (File/createTempFile (str (UUID/randomUUID)) ".txt")]

    ; AND a file with existing content
    (spit file "whatever")

    ; AND a file sink to the file that WILL append
    (let [append-sink (channel/sink file {:append true})]
      (channel/to! input-ch [append-sink])

      ; WHEN data arrives in the input channel
      (doseq [x [0 1]]
        (async/>!! input-ch x))

      ; THEN the same data is appended to the existing content in the file sink
      (is-eventually? (= "whatever0\n1\n" (slurp file))))

    ; BUT when a file sink to the file is created that will NOT append
    (let [no-append-sink (channel/sink file {:append false})]
      (channel/to! input-ch [no-append-sink])

      ; WHEN data arrives in the input channel
      (doseq [x [2 3]]
        (async/>!! input-ch x))

      ; THEN the last piece of data replaces the existing content in the file sink
      (is-eventually? (not (clojure.string/includes?
                             (slurp file)
                             "whatever"))))))