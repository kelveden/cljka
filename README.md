# cljka

![cljka](https://github.com/kelveden/cljka/actions/workflows/trunk-build.yaml/badge.svg)

A Clojure REPL-based tool for working with kafka.

## Usage

To start a new REPL: `make` or `make repl`.

## Configuration

Configuration is held in the `$HOME/.config/cljka/config.edn` file. If this file is missing or invalid you will be
informed of this upon starting the REPL. In essence, the configuration file just defines a map
of [Kafka configuration attributes](https://kafka.apache.org/documentation.html#configuration) (albeit
keywordized) that are used to configure all the kafka objects that cljka uses.

All `cljka.core` functions take one or more of the following entities as an argument:

* (Mandatory) An _environment_ keyword linking to a map of _kafka configuration attributes_ specific to a single
  environment (e.g. "test", "production" etc.) defined in the config file.
* A _topic_. This can be as simple as a string indicating a topic name or can be keyword linking to a topic
  configuration.
* A _principal_ keyword linking to an extra map of _kafka configuration attributes_ that should be merged with the
  configuration defined by the _environment_.

When executing a `cljka.core` function, cljka will build up a configuration map to pass to all Java Kafka objects it
uses. That map is built as follows:

1. Take the `:kafka` config map for the specified _environment_.
2. If a _topic_ keyword is specified as an argument AND that _topic_ is configured with a `:principal`, merge in
   the `:kafka` configuration for that principal.
3. If a _principal_ is explicitly specified (i.e. with the `with-principal` function), merge in the `:kafka`
   configuration for that principal. Overrides any principal specified at the topic level.

### Basic configuration

The simplest configuration looks like this:

```clojure
{:environments {:nonprod {:kafka {:bootstrap.servers "localhost:1111"}}
                :prod    {:kafka {:bootstrap.servers "localhost:2222"}}}}
```

Notes:

* Two environments, `:nonprod` and `:prod`, are defined.

...and the same configuration with some simple topic aliases:

```clojure
{:environments {:nonprod {:kafka {:bootstrap.servers "localhost:1111"}}
                :prod    {:kafka {:bootstrap.servers "localhost:2222"}}}
 :topics       {:topic1 {:name "topic1"}
                :topic2 {:name "topic2"}}}
```

### Advanced configuration

Below is a more complex configuration:

```clojure
{:environments {:nonprod {:kafka      {:bootstrap.servers   "localhost:1111"
                                       :security.protocol   "ssl"
                                       :ssl.key.password    ""
                                       :ssl.keystore.type   "pkcs12"
                                       :schema.registry.url "http://localhost:2222"}
                          :principals {:user1 {:kafka {:ssl.keystore.location   "/home/myuser/.config/cljka/confluent/nonprod/client.keystore.p12"
                                                       :ssl.keystore.password   ""
                                                       :ssl.truststore.location "/home/myuser/.config/cljka/confluent/nonprod/client.truststore.jks"
                                                       :ssl.truststore.password "password"}}}
                          :topics     {:topic2 {:name      "topic2-nonprod"
                                                :principal :user1}}}
                {:prod {:kafka      {:bootstrap.servers   "localhost:3333"
                                     :security.protocol   "ssl"
                                     :ssl.key.password    ""
                                     :ssl.keystore.type   "pkcs12"
                                     :schema.registry.url "http://localhost:4444"}
                        :principals {:user1 {:kafka {:ssl.keystore.location   "/home/myuser/.config/cljka/confluent/prod/client.keystore.p12"
                                                     :ssl.keystore.password   ""
                                                     :ssl.truststore.location "/home/myuser/.config/cljka/confluent/prod/client.truststore.jks"
                                                     :ssl.truststore.password "password"}}}
                        :topics     {:topic2 {:name      "topic2-prod"
                                              :principal :user1}}}}}
 :topics       {:topic1 {:name      "topic1"
                         :principal :user1}}}
```

Notes:

* SSL connectivity is configured at an environment level. For more information on how to create your own truststore and
  keystore see [here](https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6er/index.html).
* Principals are defined at an environment level.
* Topics are defined at both the environment _and_ root level - topics defined at the root level are available in _all_
  environments.
* The principal referred to in the _root_ level topic configuration is defined at the _environment_ level in all
  environments.

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

### Deserializing avro as EDN
Note that the [KafkaAvroDeserializer](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroDeserializer.java)
will just return the JSON payload wrapped in an object. Typically, when working with Clojure you'll want to view it as EDN instead.
cljka provides a post-deserialization configuration for doing this that can be defined at the root, environment or even topic level:

```clojure
{:deserialization
 {:json? true}}
```

Adding this configuration mean that any value deserialized will automatically be stringified and then parsed as JSON. It's
designed to be used with the `KafkaAvroDeserializer` but will potentially work with other deserializers too.