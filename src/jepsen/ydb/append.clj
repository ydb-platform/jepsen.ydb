(ns jepsen.ydb.append
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable])
  (:import (java.util ArrayList)
           (com.google.protobuf ByteString)
           (tech.ydb.table.query Params)
           (tech.ydb.table.values PrimitiveValue)))

(def ^:dynamic *ballast* (ByteString/copyFromUtf8 ""))

(defn new-ballast
  "Creates a new ballast value with size bytes"
  [size]
  (ByteString/copyFromUtf8 (.repeat "x" size)))

(defmacro with-ballast
  "Runs body with *ballast* bound to the specified ballast value"
  [ballast & body]
  `(binding [*ballast* ~ballast]
     (do ~@body)))

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defn drop-initial-tables
  [test table-client]
  (info "dropping initial tables")
  (conn/with-session [session table-client]
    (let [query (str "DROP TABLE IF EXISTS `" (:db-table test) "`;")]
      (conn/execute-scheme! session query))))

(defn create-initial-tables
  [test table-client]
  (info "creating initial tables")
  (conn/with-session [session table-client]
    ; TODO: configurable partition size and initial partitioning
    (let [query (str "CREATE TABLE `" (:db-table test) "` (
                          key Int64,
                          index Int64,
                          value Int64,
                          ballast string,
                          PRIMARY KEY (key, index))
                      WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED,
                            AUTO_PARTITIONING_BY_LOAD = ENABLED,
                            AUTO_PARTITIONING_PARTITION_SIZE_MB = 10,
                            PARTITION_AT_KEYS = (
                                11, 21, 31, 41, 51, 61, 71, 81, 91, 101,
                                111, 121, 131, 141, 151, 161, 171, 181, 191, 201,
                                211, 221, 231, 241, 251, 261, 271, 281, 291, 301));")]
      (conn/execute-scheme! session query))))

(defn execute-list-read
  "Executes a list read for the given key k.
   Works only when the list has at most 1k values."
  [test tx k]
  (let [query (str "DECLARE $key AS Int64;
                    SELECT index, value FROM `" (:db-table test) "`
                    WHERE key = $key
                    ORDER BY index;")
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        query-result (conn/execute! tx query params)
        rs (. query-result getResultSet 0)
        result (ArrayList.)]
    (while (. rs next)
      (let [index (-> rs (.getColumn 0) .getInt64)
            value (-> rs (.getColumn 1) .getInt64)
            expectedIndex (.size result)]
        (assert (<= index expectedIndex) "List indexes are not distinct or not ordered correctly")
        ; In the unlikely case some indexes are missing fill those with nils
        (when (< index expectedIndex)
          (dotimes [_ (- expectedIndex index)]
            (. result add nil)))
        (. result add value)))
    (when (.isTruncated rs)
      (. result add :truncated))
    (vec result)))

(defn execute-list-append
  [test tx k v]
  (let [query (str "DECLARE $key AS Int64;
                    DECLARE $value AS Int64;
                    DECLARE $ballast AS Bytes;
                    $next_index = (SELECT COALESCE(MAX(index) + 1, 0) FROM `" (:db-table test) "` WHERE key = $key);
                    UPSERT INTO `" (:db-table test) "` (key, index, value, ballast) VALUES ($key, $next_index, $value, $ballast);")
        params (Params/of "$key" (PrimitiveValue/newInt64 k)
                          "$value" (PrimitiveValue/newInt64 v)
                          "$ballast" (PrimitiveValue/newBytes *ballast*))]
    (conn/execute! tx query params)))

(defn apply-mop!
  [test tx [f k v :as mop]]
  (case f
    :r [f k (execute-list-read test tx k)]
    :append (do
              (execute-list-append test tx k v)
              mop)))

(defn apply-mop-with-auto-commit-last!
  [test tx mop index count]
  (when (= index (dec count))
    (conn/auto-commit! tx))
  (apply-mop! test tx mop))

(defrecord Client [transport table-client ballast setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          table-client (conn/open-table-client transport)]
      (assoc this :transport transport :table-client table-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (drop-initial-tables test table-client)
     (create-initial-tables test table-client)))

  (invoke! [_ test op]
    ;; (info "processing op:" op)
    (with-ballast ballast
      (debug-info/with-debug-info
        (conn/with-errors op
          (conn/with-session [session table-client]
            (conn/with-transaction [tx session]
              (let [txn (:value op)
                    txn' (mapv (partial apply-mop-with-auto-commit-last! test tx)
                               txn
                               (iterate inc 0)
                               (repeat (count txn)))
                    op' (assoc op :type :ok, :value txn')]
                op')))))))

  (teardown! [this test])

  (close! [this test]
    (.close table-client)
    (.close transport)))

(defn new-client
  [opts]
  (Client. nil nil (new-ballast (:ballast-size opts)) (atom false)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:key-count
                                              :min-txn-length
                                              :max-txn-length
                                              :max-writes-per-key])
                           :consistency-models [:ydb-serializable])))
      (assoc :client (new-client opts))))
