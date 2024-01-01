# cljka

![cljka](https://github.com/kelveden/cljka/actions/workflows/trunk-build.yaml/badge.svg)

A Clojure REPL-based tool for working with kafka.

## Serialization/deserialization

Several serializers/deserializers are supported out of the box:

* All serializers/deserializers defined
  in [org.apache.kafka.common.serialization](https://kafka.apache.org/36/javadoc/org/apache/kafka/common/serialization/package-summary.html).
* The serializers/deserializers for [Apache Avro](https://avro.apache.org/docs/) created
  by [Confluent](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html).
* [`cljka.serialization.NoopSerializer`](./src/cljka/serialization.clj)/[`cljka.deserialization.NoopDeserializer`](./src/cljka/deserialization.clj) -
  a "no operation" (de)serializer.
* [`cljka.serialization.NippySerializer`](./src/cljka/serialization.clj)/[`cljka.deserialization.NippyDeserializer`](./src/cljka/deserialization.clj) -
  (de)serializer based on [Nippy](https://github.com/taoensso/nippy).

Any of these _fully qualified_ classes can be used as the value for each of the following properties in the `cljka`
configuration:

* [`key.serializer`](https://kafka.apache.org/documentation/#producerconfigs_key.serializer)
* [`value.serializer`](https://kafka.apache.org/documentation/#producerconfigs_value.serializer)
* [`key.deserializer`](https://kafka.apache.org/documentation/#consumerconfigs_key.deserializer)
* [`value.deserializer`](https://kafka.apache.org/documentation/#consumerconfigs_value.deserializer)