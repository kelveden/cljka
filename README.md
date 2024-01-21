# cljka

![cljka](https://github.com/kelveden/cljka/actions/workflows/trunk-build.yaml/badge.svg)

A Clojure REPL-based tool for working with kafka.

## Usage

To start a new REPL: `make` or `make repl`.

## Serialization/deserialization

Several serializers/deserializers are supported out of the box:

* All serializers/deserializers defined
  in [org.apache.kafka.common.serialization](https://kafka.apache.org/36/javadoc/org/apache/kafka/common/serialization/package-summary.html).
* The serializers/deserializers for [Apache Avro](https://avro.apache.org/docs/) created
  by [Confluent](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html).

Any of these _fully qualified_ classes can be used as the value for each of the following properties in the `cljka`
configuration:

* [`key.serializer`](https://kafka.apache.org/documentation/#producerconfigs_key.serializer)
* [`value.serializer`](https://kafka.apache.org/documentation/#producerconfigs_value.serializer)
* [`key.deserializer`](https://kafka.apache.org/documentation/#consumerconfigs_key.deserializer)
* [`value.deserializer`](https://kafka.apache.org/documentation/#consumerconfigs_value.deserializer)