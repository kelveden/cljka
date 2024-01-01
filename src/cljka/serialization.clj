(ns cljka.serialization
  (:require [taoensso.nippy :as nippy]))


;------------------------------------------------
; NippySerializer
;------------------------------------------------


(gen-class
  :name cljka.serialization.NippySerializer
  :implements [org.apache.kafka.common.serialization.Serializer]
  :main false
  :prefix "nippy-"
  :methods [])

(defn nippy-configure [_ _ _])
(defn nippy-serialize [_ _ _ data] (nippy/freeze data))
(defn nippy-close [_])


;------------------------------------------------
; NoopSerializer
;------------------------------------------------


(gen-class
  :name cljka.serialization.NoopSerializer
  :implements [org.apache.kafka.common.serialization.Serializer]
  :main false
  :prefix "noop-"
  :methods [])

(defn noop-configure [_ _ _])
(defn noop-serialize [_ _ _ data] data)
(defn noop-close [_])
