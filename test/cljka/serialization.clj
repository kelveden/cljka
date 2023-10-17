(ns cljka.serialization
  (:import (org.apache.kafka.common.serialization Serializer)))

(deftype NoopSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ _] nil)
  (close [_]))
