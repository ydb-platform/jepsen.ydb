(ns jepsen.ydb.kafka-client
  (:require [clojure.string :as str]
            [jepsen.util :as util])
  (:import (java.time Duration)
           (java.util Properties)
           (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerConfig)
           (org.apache.kafka.common KafkaException)
           (org.apache.kafka.common.errors AuthorizationException
                                            DisconnectException
                                            InterruptException
                                            InvalidProducerEpochException
                                            InvalidTopicException
                                            NetworkException
                                            NotLeaderOrFollowerException
                                            OutOfOrderSequenceException
                                            ProducerFencedException
                                            TimeoutException
                                            UnknownServerException
                                            UnknownTopicOrPartitionException
                                            UnsupportedVersionException)))

(def serializer "org.apache.kafka.common.serialization.LongSerializer")
(def deserializer "org.apache.kafka.common.serialization.LongDeserializer")

(def run-id
  "Makes transactional ids unique across test runs."
  (str (random-uuid)))

(def next-transactional-id (atom -1))

(defn new-transactional-id
  []
  (str "jepsen-" run-id "-" (swap! next-transactional-id inc)))

(def next-group-id (atom -1))

(defn new-group-id
  "A fresh, unique Kafka consumer group id. sendOffsetsToTransaction requires
   a group id even for assign-only consumers (it's how it obtains
   ConsumerGroupMetadata), but the id must not be shared across clients: after
   a plain .assign() without :seek-to-beginning?, the Kafka client resolves
   each partition's fetch position by first fetching the group's last
   committed offset, falling back to auto.offset.reset only when there isn't
   one. A group id shared across jepsen workers would let one worker's assign
   silently jump to an offset committed by a different worker's transaction,
   instead of starting at auto.offset.reset=earliest -- surfacing as
   int-poll-skip/unseen. Called once per client open (not once per test), so
   a :crash-driven reopen also gets a clean group with no stale commits."
  []
  (str "jepsen-" run-id "-" (swap! next-group-id inc)))

(defn ^Properties ->properties
  [m]
  (doto (Properties.)
    (.putAll (util/map-vals str m))))

(defn bootstrap-servers
  [test node]
  (str node ":" (:kafka-port test)))

(defn jaas-config
  [mechanism username password]
  (let [login-module (case mechanism
                       "PLAIN" "org.apache.kafka.common.security.plain.PlainLoginModule"
                       ("SCRAM-SHA-256" "SCRAM-SHA-512")
                       "org.apache.kafka.common.security.scram.ScramLoginModule")]
    (format "%s required username=\"%s\" password=\"%s\";" login-module username password)))

(defn plain-username
  "YDB is multi-tenant and the Kafka wire protocol has no notion of database,
   so which database a topic lives in is conveyed via the SASL PLAIN username
   as user@database (required only for PLAIN; see
   https://ydb.tech/docs/en/reference/kafka-api/auth). Without this, the
   proxy resolves topics against some other (default) database, and produce
   fails with UNKNOWN_TOPIC_OR_PARTITION. Passing a username that already
   contains @ overrides this."
  [test]
  (let [username (or (:kafka-username test) "jepsen")]
    (if (str/includes? username "@")
      username
      (str username "@" (:db-name test)))))

(defn ydb-username
  "The actual YDB identity to CREATE/GRANT/authenticate as -- the inverse of
   plain-username: --kafka-username may already contain @database (see
   plain-username), but that suffix is only meaningful in the SASL PLAIN wire
   value, not as part of a YDB user name. Using it unstripped in YQL either
   fails to parse or creates a user that the server can never match against
   the un-suffixed name it extracts from the SASL login."
  [test]
  (let [username (or (:kafka-username test) "jepsen")]
    (first (str/split username #"@" 2))))

(defn auth-config
  "Client properties for SASL authentication. Enabled by default (as
   SASL_PLAINTEXT, no TLS) with a synthetic user, since YDB's anonymous auth
   accepts any credentials but PLAIN still needs a username to route to the
   right database, see plain-username. Disable with --no-kafka-sasl for
   clusters that don't need it."
  [test]
  (if-not (:kafka-sasl? test)
    {}
    (let [mechanism (:kafka-sasl-mechanism test)
          username  (if (= mechanism "PLAIN")
                      (plain-username test)
                      (or (:kafka-username test) "jepsen"))
          password  (or (:kafka-password test)
                        (throw (IllegalArgumentException. "kafka-sasl? is set but no :kafka-password was provided")))]
      {"security.protocol" "SASL_PLAINTEXT"
       "sasl.mechanism"    mechanism
       "sasl.jaas.config"  (jaas-config mechanism username password)})))

(defn producer-config
  [test node transactional-id]
  (merge
   (cond-> {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG                    (bootstrap-servers test node)
            ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG                 serializer
            ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG               serializer
            ; YDB's Kafka API doesn't support compression.
            ProducerConfig/COMPRESSION_TYPE_CONFIG                     "none"
            ProducerConfig/ACKS_CONFIG                                 "all"
            ; Must stay below --kafka-transaction-timeout-ms (see ydb.clj): if a
            ; transaction outlives a send that's still legitimately retrying,
            ; the coordinator can abort it out from under the producer.
            ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG                  15000
            ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG                   5000
            ProducerConfig/MAX_BLOCK_MS_CONFIG                         15000
            ProducerConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG             1000
            ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG   500
            ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000}
     transactional-id
     (assoc ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG true
            ProducerConfig/TRANSACTIONAL_ID_CONFIG   transactional-id
            ProducerConfig/TRANSACTION_TIMEOUT_CONFIG (:kafka-transaction-timeout-ms test)))
   (auth-config test)))

(defn consumer-config
  [test node group-id]
  (merge
   {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG                    (bootstrap-servers test node)
    ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG               deserializer
    ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG             deserializer
    ConsumerConfig/GROUP_ID_CONFIG                             group-id
    ConsumerConfig/ISOLATION_LEVEL_CONFIG                      (:kafka-isolation-level test)
    ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG                   false
    ; YDB's Kafka API doesn't support CRC checks; the client's default
    ; check.crcs=true throws CorruptRecordException on every fetch.
    ConsumerConfig/CHECK_CRCS_CONFIG                           false
    ConsumerConfig/AUTO_OFFSET_RESET_CONFIG                    "earliest"
    ConsumerConfig/METADATA_MAX_AGE_CONFIG                     60000
    ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG                   10000
    ConsumerConfig/DEFAULT_API_TIMEOUT_MS_CONFIG               10000
    ConsumerConfig/CONNECTIONS_MAX_IDLE_MS_CONFIG              60000
    ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG   500
    ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000}
   (auth-config test)))

(defn close-producer!
  [^KafkaProducer p]
  (.close p (Duration/ofMillis 0)))

(defn close-consumer!
  [^KafkaConsumer c]
  (.close c (Duration/ofMillis 0)))

(defn open-consumer
  [test node group-id]
  (KafkaConsumer. (->properties (consumer-config test node group-id))))

(defn open-producer
  "Opens a producer. Does not call initTransactions even when transactional-id
   is set -- see init-transactions!."
  [test node transactional-id]
  (KafkaProducer. (->properties (producer-config test node transactional-id))))

(defn init-transactions!
  "Initializes transactions on a producer opened with a transactional-id.
   Deliberately not done as part of open-producer: jepsen calls client/open!
   for every worker concurrently (jepsen.core/with-client+nemesis-setup-teardown
   uses real-pmap), so a producer that authenticates eagerly in open! can race
   against another worker's client/setup! that's still provisioning the SASL
   user -- see kafka-topic/ensure-user!. Call this instead on the first actual
   transactional op, once client/setup! is guaranteed to have completed for
   every worker."
  [^KafkaProducer producer]
  (try (.initTransactions producer)
       (catch Throwable t
         (close-producer! producer)
         (throw t))))

(defmacro unwrap-errors
  "Kafka may wrap its exceptions in an ExecutionException (future gets);
   rethrows the underlying KafkaException instead."
  [& body]
  `(try ~@body
        (catch ExecutionException e#
          (let [cause# (util/ex-root-cause e#)]
            (if (instance? KafkaException cause#)
              (throw cause#)
              (throw e#))))))

(defmacro with-errors
  "Evaluates body, which should produce a completed op, converting known Kafka
   exceptions into :fail/:info completions. `op` is an expression evaluated
   only when an exception is caught, so it can capture partial progress.
   Unrecognized exceptions are rethrown."
  [op & body]
  `(try (unwrap-errors ~@body)
        (catch AuthorizationException _#
          (assoc ~op :type :fail, :error :authorization, :end-process? true))

        (catch DisconnectException e#
          (assoc ~op :type :info, :error [:disconnect (.getMessage e#)]))

        (catch InvalidProducerEpochException e#
          (assoc ~op :type :fail, :error [:invalid-producer-epoch (.getMessage e#)]))

        (catch InvalidTopicException _#
          (assoc ~op :type :fail, :error :invalid-topic))

        (catch NetworkException e#
          (assoc ~op :type :info, :error [:network (.getMessage e#)]))

        ; Surprisingly not a definite failure, see KAFKA-13574.
        (catch NotLeaderOrFollowerException _#
          (assoc ~op :type :info, :error :not-leader-or-follower))

        (catch OutOfOrderSequenceException _#
          (assoc ~op :type :fail, :error :out-of-order-sequence, :end-process? true))

        (catch ProducerFencedException _#
          (assoc ~op :type :fail, :error :producer-fenced, :end-process? true))

        (catch UnknownTopicOrPartitionException _#
          (assoc ~op :type :fail, :error :unknown-topic-or-partition))

        (catch UnknownServerException e#
          (assoc ~op :type :info, :error [:unknown-server-exception (.getMessage e#)]))

        (catch UnsupportedVersionException e#
          (assoc ~op :type :fail, :error [:unsupported-version (.getMessage e#)], :end-process? true))

        (catch InterruptException _#
          (assoc ~op :type :info, :error :interrupted))

        (catch TimeoutException _#
          (assoc ~op :type :info, :error :kafka-timeout))

        (catch IllegalStateException e#
          (if (re-find #"Invalid transition attempted" (str (.getMessage e#)))
            (assoc ~op :type :info, :error [:illegal-transition (.getMessage e#)])
            (throw e#)))

        (catch KafkaException e#
          (let [msg# (str (.getMessage e#))]
            (cond
              (re-find #"broker is not available" msg#)
              (assoc ~op :type :fail, :error :broker-not-available)

              (re-find #"Cannot execute transactional method because we are in an error state" msg#)
              (assoc ~op :type :fail, :error [:txn-in-error-state msg#], :end-process? true)

              (re-find #"Topic or Partition .+? does not exist" msg#)
              (assoc ~op :type :fail, :error [:topic-partition-does-not-exist msg#])

              (re-find #"Unexpected error in AddOffsetsToTxnResponse" msg#)
              (assoc ~op :type :fail, :error [:add-offsets msg#])

              (re-find #"Unexpected error in TxnOffsetCommitResponse" msg#)
              (assoc ~op :type :fail, :error [:txn-offset-commit msg#])

              :else
              (assoc ~op :type :info, :error [:kafka-exception msg#]))))))
