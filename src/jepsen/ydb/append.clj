(ns jepsen.ydb.append
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
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
    (let [query (format "DROP TABLE IF EXISTS `%1$s`;" (:db-table test))]
      (conn/execute-scheme! session query))))

(defn generate-partition-at-keys
  "Generates a comma-separated list of partitioning keys"
  [test]
  (let [keys (:initial-partition-keys test)
        count (:initial-partition-count test)]
    (->> (iterate inc 1) ; 1, 2, 3, ...
         (map #(+ (* % keys) 1)) ; 11, 21, 31, ...
         (take (dec count))
         (str/join ", "))))

(defn create-initial-tables
  [test table-client]
  (info "creating initial tables")
  (conn/with-session [session table-client]
    (let [query (format "CREATE TABLE `%1$s` (
                             key Int64,
                             index Int64,
                             value Int64,
                             ballast string,
                             PRIMARY KEY (key, index))
                         WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED,
                               AUTO_PARTITIONING_BY_LOAD = ENABLED,
                               AUTO_PARTITIONING_PARTITION_SIZE_MB = %2$d,
                               PARTITION_AT_KEYS = (%3$s));"
                        (:db-table test)
                        (:partition-size-mb test)
                        (generate-partition-at-keys test))]
      (conn/execute-scheme! session query))))

(defn list-read-query
  [test]
  (format "DECLARE $key AS Int64;
           SELECT index, value FROM `%1$s`
           WHERE key = $key
           ORDER BY index"
          (:db-table test)))

(defn execute-list-read
  "Executes a list read for the given key k.
   Works only when the list has at most 1k values."
  [test tx k]
  (let [query (list-read-query test)
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

(defn list-append-query
  [test]
  (format "DECLARE $key AS Int64;
           DECLARE $value AS Int64;
           DECLARE $ballast AS Bytes;
           $next_index = (SELECT COALESCE(MAX(index) + 1, 0) FROM `%1$s` WHERE key = $key);
           UPSERT INTO `%1$s` (key, index, value, ballast) VALUES ($key, $next_index, $value, $ballast);"
          (:db-table test)))

(defn execute-list-append
  [test tx k v]
  (let [query (list-append-query test)
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
