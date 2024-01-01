(ns cljka.deserialization
  (:require [taoensso.nippy :as nippy]))


;------------------------------------------------
; NippyDeserializer
;------------------------------------------------


(gen-class
  :name cljka.deserialization.NippyDeserializer
  :implements [org.apache.kafka.common.serialization.Deserializer]
  :main false
  :prefix "nippy-"
  :methods [])

(defn nippy-configure [_ _ _])
(defn nippy-deserialize [_ _ _ data] (nippy/thaw data))
(defn nippy-close [_])


;------------------------------------------------
; NoopDeserializer
;------------------------------------------------


(gen-class
  :name cljka.deserialization.NoopDeserializer
  :implements [org.apache.kafka.common.serialization.Deserializer]
  :main false
  :prefix "noop-"
  :methods [])

(defn noop-configure [_ _ _])
(defn noop-deserialize [_ _ _ data] data)
(defn noop-close [_])
