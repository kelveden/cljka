(ns cljka.channel
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log])
  (:import (clojure.lang Atom)))

(s/def ::channel #(satisfies? async-protocols/Channel %))

(defn closed?
  "Returns a boolean indicating whether the specified channel is closed."
  [ch]
  (async-protocols/closed? ch))

(defn- loop-channel-to-sink
  [channel sink-fn {:keys [pred n] :or {pred any?
                                        n    nil}}]
  (try
    (loop [next            (async/<!! channel)
           processed-count 0]
      (when next
        (let [process?  (pred next)
              new-count (cond-> processed-count process? inc)]
          (when process? (sink-fn next))

          ; Limit looping to n invocations of f
          (when (or (nil? n) (< new-count n))
            (recur (async/<!! channel) new-count)))))

    (catch Exception e
      (log/error e))))

(defmulti sink
          "Creates a new sink channel to the specified target.

          | key                  | default | description |
          |:---------------------|:--------|:------------|
          | `:n`                 | `nil`   | A limit to the number of messages that will be written to the sink before it closes. |"
          type)

(defmethod sink Atom [^Atom a & opts]
  (let [ch (async/chan)]
    (future
      (loop-channel-to-sink ch #(swap! a conj %) opts)
      (println :atom-sink-closed))
    ch))

(defn to!
  "Starts sending data from the specified input channel to the specified sink(s).

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:close-sinks?`      | `true`  | Whether to close all sinks once the input channel closes. |"
  [channel sinks & {:keys [close-sinks?]
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