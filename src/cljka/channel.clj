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
  [sink-channel sink-fn {:keys [pred n] :or {pred any?
                                             n    nil}}]
  (try
    (loop [next            (async/<!! sink-channel)
           processed-count 0]
      (when next
        (let [process?  (pred next)
              new-count (cond-> processed-count process? inc)]
          (when process?
            (sink-fn next))

          ; Limit looping to n invocations of f
          (when (and (some? n) (>= new-count n))
            (async/close! sink-channel))

          (when-not (closed? sink-channel)
            (recur (async/<!! sink-channel) new-count)))))

    (catch Exception e
      (log/error e))))

(defmulti
  sink-chan
  "Wraps the given sink target in a channel to sink messages to.

  The first argument is the sink target. Target types supported out of the box are:

  * `clojure.lang.Atom` - the dereferenced value should be a collection
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

(defmethod sink-chan Atom
  [^Atom a & [opts]]
  (let [sink-channel (async/chan)]
    (future
      (recur-to-sink! sink-channel #(swap! a conj %) opts))
    sink-channel))

(defmethod sink-chan Writer
  [^Writer w & [{:keys [serializer]
                 :or   {serializer pprint}
                 :as   opts}]]
  (let [sink-channel (async/chan)]
    (future
      (try
        (recur-to-sink! sink-channel #(serializer % w) opts)
        (finally
          (.close w))))
    sink-channel))

(defmethod sink-chan File
  [^File f & [opts]]
  (let [w (apply io/writer (->> (into [] opts)
                                (flatten)
                                (cons f)))]
    (sink-chan w opts)))

(defmethod sink-chan String
  [^String filename & [opts]]
  (sink-chan (io/file filename) opts))

(defn to-chan!
  "Starts piping data from the specified input channel to the specified sink channel.

  | key                 | default | description |
  |:--------------------|:--------|:------------|
  | `:close-sink?`      | `true`  | Whether to close the sink channel once the input channel closes. |"
  [in-channel sink-channel & [{:keys [close-sink?]
                               :or   {close-sink? true}}]]
  (async/pipe in-channel sink-channel close-sink?))

(defn to!
  "Starts piping data from the specified input channel to the specified sink target. For more control over the
  sink channel itself, wrap the sink target use to-chan! instead.

  | key                 | default | description |
  |:--------------------|:--------|:------------|
  | `:close-sink?`      | `true`  | Whether to close the sink destination once the input channel closes. |"
  [in-channel sink-target & [{:keys [close-sink?]
                              :or   {close-sink? true}}]]
  (to-chan! in-channel (sink-chan sink-target) {:close-sink? close-sink?}))

(defn close!
  "Closes the specified channel; emptying it first."
  [channel]
  (async/close! channel)
  (async/poll! channel)
  nil)

(defn poll!
  "Polls the specified channel"
  [channel]
  (async/poll! channel))