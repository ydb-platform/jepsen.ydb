(ns jepsen.ydb.kafka-topic
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.tests.kafka :as kafka]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.kafka-client :as kc])
  (:import (java.time Duration)
           (org.apache.kafka.clients.consumer ConsumerRecord
                                              ConsumerRecords
                                              KafkaConsumer
                                              OffsetAndMetadata)
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerRecord
                                              RecordMetadata)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.errors AuthorizationException
                                            InterruptException
                                            OutOfOrderSequenceException
                                            ProducerFencedException
                                            TimeoutException)
           (tech.ydb.core StatusCode UnexpectedResultException)))

(def default-poll-ms 100)

(def send-timeout-ms 10000)

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

;; jepsen.tests.kafka requires the client to map integer keys bijectively onto
;; topic-partitions. YDB topics can only be created up front (via YQL), so all
;; keys live in a single topic and key k is partition k. This is bijective as
;; long as the topic has at least key-count partitions.

(defn k->topic
  [test _k]
  (:kafka-topic-name test))

(defn k->partition
  [_test k]
  (int k))

(defn k->topic-partition
  [test k]
  (TopicPartition. ^String (k->topic test k) (int (k->partition test k))))

(defn topic-partition->k
  [_test ^TopicPartition tp]
  (long (.partition tp)))

(defn drop-topic!
  [test query-client]
  (info "dropping kafka topic")
  (conn/with-session [session query-client]
    (try
      (conn/execute-scheme! session (format "DROP TOPIC `%s`;" (:kafka-topic-name test)))
      (catch UnexpectedResultException e
        (when-not (= (-> e .getStatus .getCode) StatusCode/SCHEME_ERROR)
          (throw e))))))

(defn create-topic!
  "Creates a topic with a fixed number of partitions. The Kafka API can't be
   used with topics that have auto partitioning enabled, which is off by
   default."
  [test query-client]
  (info "creating kafka topic")
  (conn/with-session [session query-client]
    (conn/execute-scheme!
     session
     (format "CREATE TOPIC `%1$s` WITH (min_active_partitions = %2$d, max_active_partitions = %2$d);"
             (:kafka-topic-name test) (:kafka-partition-count test)))))

(defn ensure-user!
  "(Re-)creates the YDB user the Kafka client authenticates as, and grants it
   full rights on the database. The Kafka wire protocol has no notion of
   database, so on YDB the target database is conveyed via the SASL PLAIN
   username as user@database; that still needs to be a real, authenticated
   user even when the cluster has anonymous access enabled for plain
   (non-Kafka) connections."
  [test query-client]
  (info "creating kafka api user")
  (let [username (kc/ydb-username test)
        password (:kafka-password test)]
    (conn/with-session [session query-client]
      (conn/execute-scheme! session (format "DROP USER IF EXISTS %s;" username))
      (conn/execute-scheme! session (format "CREATE USER %s PASSWORD '%s';" username password))
      (conn/execute-scheme! session (format "GRANT ALL ON `%s` TO %s;" (:db-name test) username)))))

(defn polled-entries
  "All [k [[offset value] ...]] entries from the :poll micro-ops in a txn value."
  [value]
  (for [[f polled] value
        :when (and (= :poll f) (map? polled))
        entry polled]
    entry))

(defn first-polled-offsets
  "Map of key to the first offset polled for that key in a txn value."
  [value]
  (reduce (fn [offsets [k pairs]]
            (if (contains? offsets k)
              offsets
              (assoc offsets k (ffirst pairs))))
          {}
          (polled-entries value)))

(defn highest-polled-offsets
  "Map of key to the highest offset polled for that key in a txn value."
  [value]
  (reduce (fn [offsets [k pairs]]
            (reduce (fn [offsets [offset _]]
                      (update offsets k (fnil max Long/MIN_VALUE) offset))
                    offsets
                    pairs))
          {}
          (polled-entries value)))

(defn mop!
  "Applies a :poll or :send micro-operation."
  [test {:keys [^KafkaProducer producer ^KafkaConsumer consumer]} poll-ms mop]
  (case (first mop)
    :poll (try
            (let [^ConsumerRecords records (.poll consumer (Duration/ofMillis poll-ms))]
              (->> (.partitions records)
                   (map (fn [^TopicPartition tp]
                          [(topic-partition->k test tp)
                           (mapv (fn [^ConsumerRecord record]
                                   [(.offset record) (.value record)])
                                 (.records records tp))]))
                   (into (sorted-map))
                   (vector :poll)))
            (catch IllegalStateException e
              (if (re-find #"not subscribed to any" (str (.getMessage e)))
                [:poll {}]
                (throw e))))

    :send (let [[f k v] mop
                record (ProducerRecord. ^String (k->topic test k)
                                        (int (k->partition test k))
                                        nil
                                        v)
                ^RecordMetadata res (or (deref (.send producer record) send-timeout-ms nil)
                                        (throw (TimeoutException.
                                                "Timed out waiting for send acknowledgement")))
                offset (when (.hasOffset res)
                         (.offset res))]
            [f k [offset v]])))

(defn send-offsets!
  "Sends the highest offsets polled in a txn value to the current transaction."
  [test {:keys [^KafkaProducer producer ^KafkaConsumer consumer]} value]
  (let [offsets (highest-polled-offsets value)]
    (when (seq offsets)
      (.sendOffsetsToTransaction
       producer
       (into {}
             (map (fn [[k offset]]
                    ; The *next* offset to read, not the last one read.
                    [(k->topic-partition test k)
                     (OffsetAndMetadata. (long (inc offset)))]))
             offsets)
       (.groupMetadata consumer)))))

(defn non-abortable?
  "Errors after which the producer can't (or mustn't) abort the transaction."
  [t]
  (or (instance? ProducerFencedException t)
      (instance? OutOfOrderSequenceException t)
      (instance? AuthorizationException t)))

(defn try-abort!
  [^KafkaProducer producer]
  (try (.abortTransaction producer)
       (catch RuntimeException _ nil)))

(defn with-txn*
  "Calls body-fn. When (:txn? test) is set, does so inside a producer
   transaction which is committed afterwards, or aborted if anything throws."
  [test client body-fn]
  (if-not (:txn? test)
    (body-fn)
    (let [^KafkaProducer producer (:producer client)]
      (when (compare-and-set! (:txn-initialized? client) false true)
        (kc/init-transactions! producer))
      (.beginTransaction producer)
      (let [result (try (body-fn)
                        (catch Throwable t
                          (when-not (non-abortable? t)
                            (try-abort! producer))
                          (throw t)))]
        (try (.commitTransaction producer)
             (catch Throwable t
               ; A timed out commit may still complete, so it can't be aborted.
               (when-not (or (non-abortable? t)
                             (instance? TimeoutException t)
                             (instance? InterruptException t))
                 (try-abort! producer))
               (throw t)))
        result))))

(defn rollback-consumer!
  "Seeks the consumer back to the first offsets polled by op, so that the next
   poll doesn't skip records observed by an op that didn't succeed."
  [test ^KafkaConsumer consumer op]
  (doseq [[k offset] (first-polled-offsets (:value op))]
    (try (.seek consumer (k->topic-partition test k) (long offset))
         (catch IllegalStateException _ nil))))

(defn invoke-txn!
  [test client op]
  (let [results (atom (vec (:value op)))
        partial-op (fn [] (assoc op :value @results))
        poll-ms (:poll-ms op default-poll-ms)
        rollback! (fn [op'] (rollback-consumer! test (:consumer client) op'))
        op' (try
              (kc/with-errors (partial-op)
                (with-txn* test client
                  (fn []
                    (doseq [[i mop] (map-indexed vector (:value op))]
                      (swap! results assoc i (mop! test client poll-ms mop)))
                    (when (:txn? test)
                      (send-offsets! test client @results))
                    (assoc op :type :ok, :value @results))))
              (catch Throwable t
                (rollback! (partial-op))
                (throw t)))]
    (when-not (= :ok (:type op'))
      (rollback! op'))
    op'))

(defrecord Client [node
                   ^KafkaProducer producer
                   ^KafkaConsumer consumer
                   ; Per-client (per-worker) flag: has initTransactions been
                   ; called on this producer yet? Deliberately not done in
                   ; open! -- see kc/init-transactions!.
                   txn-initialized?
                   setup?]
  client/Client
  (open! [this test node]
    (let [producer (kc/open-producer test node (when (:txn? test)
                                                 (kc/new-transactional-id)))]
      (assoc this
             :node node
             :producer producer
             :consumer (kc/open-consumer test node (kc/new-group-id))
             :txn-initialized? (atom false))))

  (setup! [this test]
    (once-per-cluster
     setup?
     (with-open [transport (conn/open-transport test node)
                 query-client (conn/open-query-client transport)]
       (drop-topic! test query-client)
       (create-topic! test query-client)
       (when (:kafka-sasl? test)
         (ensure-user! test query-client)))))

  (invoke! [this test op]
    (case (:f op)
      :assign (let [tps (mapv (partial k->topic-partition test) (:value op))]
                (.assign consumer tps)
                (when (:seek-to-beginning? op)
                  (.seekToBeginning consumer tps))
                (assoc op :type :ok))

      ; Forces jepsen to close this client and open a new one.
      :crash (assoc op :type :info)

      ; Would need an Admin client against YDB's Kafka API. jepsen.tests.kafka
      ; expects drivers to be able to fail these, see kafka/stats-checker.
      :debug-topic-partitions (assoc op :type :info, :error :not-implemented)

      (:poll :send :txn) (invoke-txn! test this op)))

  (teardown! [this test])

  (close! [this test]
    (kc/close-producer! producer)
    (kc/close-consumer! consumer)))

(defn new-client
  [_opts]
  (Client. nil nil nil nil (atom false)))

(defn workload
  "jepsen.tests.kafka workload against YDB's Kafka API. Only :sub-via :assign
   is supported. Note that jepsen.tests.kafka ignores --max-txn-length: it
   always uses 4 when transactions are enabled and 1 otherwise."
  [opts]
  (-> (kafka/workload (-> (select-keys opts [:key-count
                                             :min-txn-length
                                             :max-writes-per-key
                                             :txn?
                                             :crash-clients?
                                             :crash-client-interval])
                          (assoc :sub-via #{:assign})))
      (assoc :client (new-client opts))))
