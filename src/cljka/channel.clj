(ns cljka.channel
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]))

(defn closed?
  "Returns a boolean indicating whether the specified channel is closed."
  [ch]
  (async-protocols/closed? ch))