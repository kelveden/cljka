(ns cljka.channel
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as log])
  (:import (clojure.lang Atom)
           (java.io Writer File)))

(s/def ::channel #(satisfies? async-protocols/Channel %))

(defn closed?
  "Returns a boolean indicating whether the specified channel is closed."
  [ch]
  (async-protocols/closed? ch))

(defn recur-to-sink!
  "Reads items from the incoming channel "
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

(defmulti sink
          "Creates a new sink channel to the specified target.

          | key     | default | description |
          |:--------|:--------|:------------|
          | `:pred` | `any?`  | A predicate to use to filter the incoming messages by. |
          | `:n`    | `nil`   | A limit to the number of messages that will be written to the sink before it closes. |"
          (fn [o & _] (type o)))

(defmethod sink Atom
  [^Atom a & [opts]]
  (let [ch (async/chan)]
    (future
      (recur-to-sink! ch #(swap! a conj %) opts))
    ch))

(defmethod sink Writer
  [^Writer w & [{:keys [printer close-after-sink?]
                 :or   {printer           pprint
                        close-after-sink? false}
                 :as   opts}]]
  (let [ch (async/chan)]
    (future
      (try
        (recur-to-sink! ch #(printer % w) opts)
        (finally
          (when close-after-sink? (.close w)))))
    ch))

(defmethod sink File
  [^File f & [opts]]
  (let [w (apply io/writer (->> (into [] opts)
                                (flatten)
                                (cons f)))]
    (sink w (assoc opts :close-after-sink? true))))

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