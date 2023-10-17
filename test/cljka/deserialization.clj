(ns cljka.deserialization
  (:import (org.apache.kafka.common.serialization Deserializer)))

(deftype NoopDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ _] nil)
  (close [_]))
