(ns cljka.channel
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as log])
  (:import (clojure.lang Atom)
           (java.io File Writer)))

(s/def ::channel #(satisfies? async-protocols/Channel %))

(defn closed?
  "Returns a boolean indicating whether the specified channel is closed."
  [ch]
  (async-protocols/closed? ch))

(defn ^:no-doc recur-to-sink!
  "Read messages from the incoming channel, writing each message to a sink via the unary sink-fn."
  [ch sink-fn {:keys [pred n] :or {pred any?
                                   n    nil}}]
  (try
    (loop [next            (async/<!! ch)
           processed-count 0]
      (when next
        (let [process?  (pred next)
              new-count (cond-> processed-count process? inc)]
          (when process?
            (sink-fn next))

          ; Limit looping to n invocations of f
          (when (and (some? n) (>= new-count n))
            (async/close! ch))

          (when-not (closed? ch)
            (recur (async/<!! ch) new-count)))))

    (catch Exception e
      (log/error e))))

(defmulti
  sink
  "Create a new channel for messages to sink to.

  The first argument is the sink target. Target types supported out of the box are:

  * `clojure.lang.Atom` - the derefed value should be a collection
  * `java.io.Writer`
  * `java.io.File`
  * `java.lang.String` - should indicate an absolute file path.

  The second argument is an option map for that sink target.

  Options supported by all sink targets:

  | key     | default | description |
  |:--------|:--------|:------------|
  | `:pred` | `any?`  | A predicate to use to filter the incoming messages by. |
  | `:n`    | `nil`   | A limit to the number of messages that will be written to the sink before it closes. |

  java.io.Writer based targets (i.e. `java.io.Writer`, `File`, `String`) also support the same additional options as for
  `io/writer` plus:

  | key                  | default  | description |
  |:---------------------|:---------|:------------|
  | `:serializer`        | `pprint` | function taking an object and `java.io.Writer` that will be used to serialize the object to the Writer. |"
  (fn [o & _] (type o)))

(defmethod sink Atom
  [^Atom a & [opts]]
  (let [ch (async/chan)]
    (future
      (recur-to-sink! ch #(swap! a conj %) opts))
    ch))

(defmethod sink Writer
  [^Writer w & [{:keys [serializer]
                 :or   {serializer pprint}
                 :as   opts}]]
  (let [ch (async/chan)]
    (future
      (try
        (recur-to-sink! ch #(serializer % w) opts)
        (finally
          (.close w))))
    ch))

(defmethod sink File
  [^File f & [opts]]
  (let [w (apply io/writer (->> (into [] opts)
                                (flatten)
                                (cons f)))]
    (sink w opts)))

(defmethod sink String
  [^String filename & [opts]]
  (sink (io/file filename) opts))

(defn to!
  "Starts sending data from the specified input channel to the specified sink(s).

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:close-sinks?`      | `true`  | Whether to close all sinks once the input channel closes. |"
  ([channel sinks {:keys [close-sinks?]
                   :or   {close-sinks? true}}]
   (future
     (try
       (loop [next (async/<!! channel)]
         (when next
           (doseq [sink sinks] (async/>!! sink next))
           (recur (async/<!! channel))))

       (println :sending-to-sinks-completed)

       (catch Exception e
         (log/error e))

       (finally
         (when close-sinks?
           (doseq [sink sinks] (async/close! sink)))))))
  ([channel sinks]
   (to! channel sinks {})))