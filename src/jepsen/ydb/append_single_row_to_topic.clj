(ns jepsen.ydb.append-single-row-to-topic
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable])
  (:import (java.nio.charset StandardCharsets)
           (java.util.concurrent ExecutionException TimeUnit)
           (tech.ydb.core StatusCode)
           (tech.ydb.core UnexpectedResultException)
           (tech.ydb.topic.settings DescribeTopicSettings
                                     ReaderSettings
                                     SendSettings
                                     TopicReadSettings
                                     WriterSettings)
           (tech.ydb.topic.write Message)))

;; A YDB topic-only analogue of append-single-row.
;;
;; Jepsen's elle append generator only guarantees key-count keys are active
;; AT ANY POINT: once a key hits max-writes-per-key, elle.txn/fresh-key
;; retires it and replaces it with a brand new, ever-increasing key (see
;; elle.txn/wr-txns). So the key space is effectively unbounded over the
;; life of a test, even though only key-count keys are live at once. A fixed
;; 1-key-per-partition mapping can't work for a topic with a fixed partition
;; count, so instead keys are hashed onto a fixed number of partitions
;; (topic-partition-count) and each message carries its logical key alongside
;; the value, so replay reads can filter a shared partition down to the
;; messages belonging to one key.
;;
;; :append writes a message to the key's partition inside the surrounding
;; YDB transaction (topic writes require an already-active tx handle, see
;; conn/ensure-tx!). :r performs a full non-destructive replay read of the
;; whole partition (offset 0..end via a consumer-less reader), filtered down
;; to the requested key, rather than tracking a committed offset -- own
;; writes aren't visible within the same transaction anyway, and we want
;; reads to be repeatable and side-effect free.

(def read-timeout-ms
  "How long a single replay read waits for a message that DescribeTopic
   already reported as durably written before treating it as an error."
  30000)

(def write-ack-timeout-ms
  "How long a single append waits for the server's WriteAck before treating
   it as an error. We must wait for this ack (not just for the message to be
   locally buffered) before letting the surrounding SQL transaction commit:
   SyncWriter/send only buffers locally and returns immediately, and
   SyncWriter/flush deliberately swallows write failures (the underlying
   WriterQueue.flush() javadoc: \"ackFuture can be failed, but flushFuture
   must be always successful\"), so either one on its own lets commit! race
   ahead of the actual topic write and finish the transaction before the
   write ever reaches the server -- the server then reports \"Transaction
   not found\" for the now-stale write. Waiting on AsyncWriter's per-message
   WriteAck future closes that race."
  30000)

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(def partition-ids-cache
  "Caches the topic's actual partition ids per topic-name, discovered once
   via DescribeTopic. We can't assume ids are a dense [0, N) range -- YDB
   partition ids aren't guaranteed to be allocated that way (observed: some
   ids computed via a plain 0-based mod were accepted by explicit-partition
   writes, but DescribeTopic never listed them), so keys are hashed onto
   whatever ids the server actually reports instead."
  (atom {}))

(defn discover-partition-ids
  [test topic-client]
  (let [description (-> topic-client
                        (.describeTopic (:topic-name test))
                        .join .getValue)]
    (->> (.getPartitions description)
         (mapv #(.getPartitionId %))
         sort
         vec)))

(defn partition-ids
  "Returns the topic's actual partition ids, discovering and caching them on
   first use."
  [test topic-client]
  (if-let [ids (get @partition-ids-cache (:topic-name test))]
    ids
    (let [ids (discover-partition-ids test topic-client)]
      (swap! partition-ids-cache assoc (:topic-name test) ids)
      ids)))

(defn hash-key-onto
  "Maps a (potentially unbounded) Jepsen key onto one of the given partition
   ids, which need not be a dense [0, N) range."
  [ids k]
  (nth ids (mod (long k) (count ids))))

(defn partition-for-key
  "Maps a (potentially unbounded) Jepsen key onto one of the topic's actual
   partitions."
  [test topic-client k]
  (hash-key-onto (partition-ids test topic-client) k))

(defn drop-initial-topic
  "Drops the test topic, ignoring the error when it doesn't exist yet."
  [test query-client]
  (info "dropping initial topic")
  (conn/with-session [session query-client]
    (try
      (conn/execute-scheme! session (format "DROP TOPIC `%1$s`;" (:topic-name test)))
      (catch UnexpectedResultException e
        (when-not (= (-> e .getStatus .getCode) StatusCode/SCHEME_ERROR)
          (throw e))))))

(defn create-initial-topic
  "Creates the test topic with a fixed number of partitions
   (topic-partition-count). Auto partitioning is left disabled (the default)
   so partition ids stay stable for the whole test."
  [test query-client]
  (info "creating initial topic")
  (swap! partition-ids-cache dissoc (:topic-name test))
  (conn/with-session [session query-client]
    (let [query (format "CREATE TOPIC `%1$s` WITH (min_active_partitions = %2$d, max_active_partitions = %2$d);"
                        (:topic-name test) (:topic-partition-count test))]
      (conn/execute-scheme! session query))))

(defn get-writer!
  "Returns the cached AsyncWriter for a partition, creating and initializing
   one on first use. Multiple keys sharing a partition share the writer.
   We use AsyncWriter (not SyncWriter) because only AsyncWriter/send returns
   a per-message CompletableFuture<WriteAck> we can wait on -- see
   write-ack-timeout-ms."
  [test topic-client writers partition-id]
  (if-let [w (get @writers partition-id)]
    w
    (let [w (-> topic-client
                (.createAsyncWriter (-> (WriterSettings/newBuilder)
                                        (.setTopicPath (:topic-name test))
                                        (.setPartitionId partition-id)
                                        .build)))]
      (-> w .init .join)
      (swap! writers assoc partition-id w)
      w)))

(defn encode-message
  "Encodes a (key, value) pair as message bytes."
  [k v]
  (.getBytes (str k "," v) StandardCharsets/UTF_8))

(defn decode-message
  "Decodes message bytes back into a [key value] pair of longs."
  [^bytes data]
  (let [[ks vs] (str/split (String. data StandardCharsets/UTF_8) #",")]
    [(Long/parseLong ks) (Long/parseLong vs)]))

(defn execute-topic-append!
  "Appends v to key k's partition, inside tx. Blocks until the server
   confirms the write (WriteAck), so the message is guaranteed to be
   registered with tx before the caller is allowed to commit it."
  [test tx topic-client writers k v]
  (let [raw-tx (conn/ensure-tx! tx)
        partition-id (partition-for-key test topic-client k)
        writer (get-writer! test topic-client writers partition-id)
        message (Message/of (encode-message k v))
        settings (-> (SendSettings/newBuilder)
                     (.setTransaction raw-tx)
                     .build)]
    (try
      (-> (.send writer message settings)
          (.get write-ack-timeout-ms TimeUnit/MILLISECONDS))
      (catch ExecutionException e
        (let [cause (.getCause e)]
          (throw (if (instance? UnexpectedResultException cause) cause e)))))))

(defn execute-topic-read!
  "Reads the list for key k via a full non-destructive replay: describes the
   topic to find the current [start, end) offset range of key k's partition,
   reads exactly that many messages with a fresh, consumer-less reader, and
   keeps only the ones tagged with key k (the partition may be shared with
   other keys). Returns nil if key k has never been appended to, or a vector
   of longs otherwise."
  [test topic-client k]
  (let [partition-id (partition-for-key test topic-client k)
        describe-settings (-> (DescribeTopicSettings/newBuilder)
                               (.withIncludeStats true)
                               .build)
        description (-> topic-client
                        (.describeTopic (:topic-name test) describe-settings)
                        .join .getValue)
        partition (->> (.getPartitions description)
                       (filter #(= (.getPartitionId %) partition-id))
                       first)
        offsets (-> partition .getPartitionStats .getPartitionOffsets)
        start (.getStart offsets)
        end (.getEnd offsets)]
    (when (> end start)
      (let [reader-settings (-> (ReaderSettings/newBuilder)
                                 .withoutConsumer
                                 (.addTopic (-> (TopicReadSettings/newBuilder)
                                               (.setPath (:topic-name test))
                                               (.setPartitionIds [partition-id])
                                               .build))
                                 .build)
            reader (.createSyncReader topic-client reader-settings)]
        (try
          (.initAndWait reader)
          (let [values (->> (range (- end start))
                            (mapv (fn [_]
                                    (let [msg (.receive reader read-timeout-ms TimeUnit/MILLISECONDS)]
                                      (when (nil? msg)
                                        (throw (ex-info "topic replay read timed out"
                                                        {:topic (:topic-name test) :key k})))
                                      (decode-message (.getData msg)))))
                            (filter (fn [[mk _]] (= mk (long k))))
                            (mapv second))]
            (when (seq values)
              values))
          (finally
            (.shutdown reader)))))))

(defn apply-mop!
  [test tx topic-client writers [f k v :as mop]]
  (case f
    :r [[f k (execute-topic-read! test topic-client k)]]
    :append [(do (execute-topic-append! test tx topic-client writers k v) mop)]))

(defrecord Client [transport query-client topic-client writers setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          query-client (conn/open-query-client transport)
          topic-client (conn/open-topic-client transport)]
      (assoc this :transport transport :query-client query-client :topic-client topic-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (drop-initial-topic test query-client)
     (create-initial-topic test query-client)))

  (invoke! [_ test op]
    (debug-info/with-debug-info
      (conn/with-errors op
        (conn/with-session [session query-client]
          (conn/with-transaction [tx [session (:model test)]]
            (let [txn (:value op)
                  txn' (->> txn
                            (mapcat (partial apply-mop! test tx topic-client writers))
                            (into []))]
              (assoc op :type :ok, :value txn')))))))

  (teardown! [this test])

  (close! [this test]
    (doseq [[_ w] @writers]
      (try
        (-> w .shutdown (.get 5 TimeUnit/SECONDS))
        (catch Exception _)))
    (.close topic-client)
    (.close query-client)
    (.close transport)))

(defn new-client
  [opts]
  (Client. nil nil nil (atom {}) (atom false)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:key-count
                                              :min-txn-length
                                              :max-txn-length
                                              :max-writes-per-key])
                           :consistency-models [(:model opts)])))
      (assoc :client (new-client opts))))
