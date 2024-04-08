(ns user
  #_{:clj-kondo/ignore [:refer-all]}
  (:require [taoensso.timbre :refer [set-min-level!]]
            [cljka.core :refer :all]
            [cljka.channel :refer :all]
            [cljka.config :refer [load-config]]))

(set-min-level! :warn)

;
;--- Cached configuration
;

(defn reload!
  "Reloads the configuration from file."
  []
  (set-config!
    (load-config)))

(reload!)

; The content below is used to fill the examples section in the README
(comment
  ;
  ; Basic stuff
  ;

  ; List all topics in alphabetical order on the broker configured for the :prod environment
  (get-topics :prod)

  ; Get vector of partitions for the account_v1 topic on the :prod environment
  (get-partitions :prod "account_v1")
  ;=> [0 1 2 3]

  ; Get vector of partitions for the topic configured with the :account alias on the :prod environment
  (get-partitions :prod :account)
  ;=> [0 1 2 3]

  ;
  ; Getting offsets
  ;

  ; Get offsets by partition for the "myconsumer" group on the :account topic in the :prod environment.
  (get-group-offsets :prod :account "myconsumer")
  ;=> [[0 141] [1 131] [2 113] [3 178]]

  ; Get the earliest offsets on the :account topic in the :prod environment
  (get-offsets-at :prod :account :start)
  ;=> [[0 0] [1 0] [2 0] [3 0]]

  ; Get the latest offsets on the :account topic in the :prod environment
  (get-offsets-at :prod :account :end)
  ;=> [[0 230] [1 223] [2 256] [3 244]]

  ; Get what were the latest offsets immediately after 23:00 on 01/01/2024
  (get-offsets-at :prod :account 1704150000000)
  ;=> [[0 24] [1 35] [2 22] [3 27]]

  ; Get the consumer lag by partition for the "myconsumer" group over the :account topic on the :prod environment
  (get-lag :prod :account "myconsumer")
  ;=> {:total 29 :by-partition [[0 4] [1 5] [2 20] [3 0]]}

  ;
  ; Setting consumer group offsets
  ;

  ; Set the consumer group offsets for the "myconsumer" group on the :account topic on the :prod environment in such a way
  ; as to reconsume everything since 23:00 on 01/01/2024 (see above)
  ; IMPORTANT: Make sure to stop any other consumers in the group first!
  (set-group-offsets! :prod :account "myconsumer" [[0 23] [1 34] [2 21] [3 26]])

  ;
  ; Consuming
  ;

  ; Open a new consumer core.async channel from the start of the :account topic on the :prod environment
  (def ch (consume! :prod :account :start))

  ; Read the next message from the consumer
  (poll! ch)
  ;=> {:key "mykey" :partition 2 :offset 1 :timestamp #time/instant"2022-09-03T05:33:29.606Z" :value "somevalue"}

  ; Consume all messages on the :account topic with a key "mykey" to an atom
  (def a (atom []))
  (to! ch a {:pred #(= "mykey" (:key %))})

  ; Consume the first 3 messages from the :account topic to the atom.
  (to! ch a {:n 3})

  ; Consume the remaining messages to the file /tmp/stuff
  (to! ch "/tmp/stuff")

  ; Close the consumer channel (any open sinks like the file sink to /tmp/stuff above will also be closed)
  (close! ch))
