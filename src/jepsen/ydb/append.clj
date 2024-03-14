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
           (tech.ydb.table.values PrimitiveValue)
           (tech.ydb.table.values StructValue)
           (tech.ydb.table.values ListValue)
           (tech.ydb.table.values Value)))

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

(defn parse-list-read-result
  "Parses a single ResultSet of a read into a vec of values"
  [rs]
  (let [result (ArrayList.)]
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

(defn execute-list-read
  "Executes a list read for the given key k.
   Works only when the list has at most 1k values."
  [test tx k]
  (let [query (list-read-query test)
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        query-result (conn/execute! tx query params)]
    (parse-list-read-result (. query-result getResultSet 0))))

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

(defn list-multi-read-and-append-query
  "Returns a query that performs read-count reads then write-count writes

   Each read has a $read<N> parameter, which would correspond to a separate result set.

   When write-count is positive a $writes (key/value) and $ballast parameters will be expected,
   each naming a separate write. Keys must be distinct."
  [test read-count write-count]
  (let [declares (concat
                  (->> (range read-count)
                       (map #(str "DECLARE $read" (inc %) " AS Int64;\n")))
                  (if (pos? write-count)
                    ["DECLARE $writes AS List<Struct<key:Int64, value:Int64>>;\n"
                     "DECLARE $ballast AS Bytes;\n"]
                    []))
        selects (->> (range read-count)
                     (map #(format "SELECT index, value FROM `%1$s` WHERE key = $read%2$d ORDER BY index;\n" (:db-table test) (inc %))))
        appends (if (pos? write-count)
                  ["UPSERT INTO `" (:db-table test) "` (\n"
                   "    SELECT w.key AS key, COALESCE(MAX(t.index) + 1, 0) AS index, w.value AS value, $ballast AS ballast\n"
                   "    FROM AS_TABLE($writes) AS w\n"
                   "    LEFT JOIN `" (:db-table test) "` AS t ON t.key = w.key\n"
                   "    GROUP BY w.key, w.value\n"
                   ");\n"]
                  [])]
    (apply str (concat declares selects appends))))

(defn make-multi-read-and-append-params
  [reads writes]
  (let [params (Params/create (+ (count reads) (if (pos? (count writes)) 2 0)))]
    (doall
     (map-indexed
      (fn [i v]
        (. params put (str "$read" (inc i)) (PrimitiveValue/newInt64 v)))
      reads))
    (when (pos? (count writes))
      (let [writeValues (map (fn [[k v]]
                               (StructValue/of "key" (PrimitiveValue/newInt64 k)
                                               "value" (PrimitiveValue/newInt64 v)))
                             writes)
            writeList (ListValue/of (into-array Value writeValues))]
        (. params put "$writes" writeList)
        (. params put "$ballast" (PrimitiveValue/newBytes *ballast*))))
    params))

(defn execute-multi-read-and-append-query
  "Executes reads (a vec of keys) and writes (a vec of key/value pairs) as a single query.

   Returns a vec of results for each read in the same order."
  [test tx reads writes]
  (let [query (list-multi-read-and-append-query test (count reads) (count writes))
        params (make-multi-read-and-append-params reads writes)
        query-result (conn/execute! tx query params)]
    (->> (range (count reads))
         (mapv #(parse-list-read-result (. query-result getResultSet %))))))

(defrecord OperationBatch [ops reads writes readmap])

(defn batch-compatible-operations
  "Combines compatible micro-ops into batches.

   When used as a (batch-compatible-operations test) returns a transducer.

   When used as a (batch-compatible-operations test coll) returns a lazy sequence transforming coll."
  ([test]
   (let [debug? (:debug-batch-compatible-operations? test false)
         batch-single-ops (:batch-single-ops test true)
         batch-ops-probability (:batch-ops-probability test 1.0)]
     (fn [rf]
       (let [ops (volatile! (transient []))
             keys (volatile! (transient #{}))
             reads (volatile! (transient []))
             writes (volatile! (transient []))
             readmap (volatile! (transient {}))
             writeset (volatile! (transient #{}))
             ; Returns true when the new op is compatible with currently batched ops
             compatible? (fn [[f k _]]
                           (case f
                             ; Can read new key unless already read or written
                             :r (not (or (contains? @readmap k) (contains? @writeset k)))
                             ; Can write new key unless previously written (read + append is ok)
                             :append (not (contains? @writeset k))))
             ; Returns true when the new op should be enqueued into the next batch
             should-enqueue? (fn [op]
                               (or (= 0 (count @ops))
                                   (and (compatible? op)
                                        (< (rand) batch-ops-probability))))
             ; Enqueues op into the next batch
             enqueue! (fn [[f k v :as op]]
                        (when debug?
                          (info "enqueue!" op))
                        (vswap! ops conj! op)
                        (vswap! keys conj! k)
                        (case f
                          :r (let [readIndex (count @reads)]
                               (vswap! reads conj! k)
                               (vswap! readmap assoc! k readIndex))
                          :append (do
                                    (vswap! writes conj! [k v])
                                    (vswap! writeset conj! k)))
                        nil)
             ; Returns current batch as a possible [:batch keys OperationBatch] operation and prepares for the next batch
             flush! (fn []
                      (let [batch (if (or batch-single-ops
                                          (not= 1 (count @ops)))
                                    [:batch
                                     (persistent! @keys)
                                     (OperationBatch.
                                      (persistent! @ops)
                                      (persistent! @reads)
                                      (persistent! @writes)
                                      (persistent! @readmap))]
                                    (get @ops 0))]
                        (when debug?
                          (info "batch-compatible-operations flush!" batch))
                        (vreset! ops (transient []))
                        (vreset! keys (transient #{}))
                        (vreset! reads (transient []))
                        (vreset! writes (transient []))
                        (vreset! readmap (transient {}))
                        (vreset! writeset (transient #{}))
                        batch))]
         (fn
           ([] (rf))
           ([result]
            (when debug?
              (info "batch-compatible-operations reduced:" result))
            (let [result (if (= 0 (count @ops))
                           result
                           (let [result (rf result (flush!))]
                             (when debug?
                               (info "batch-compatible-operations final result:" result))
                             (unreduced result)))]
              (rf result)))
           ([result op]
            (when debug?
              (info "batch-compatible-operations input:" result op))
            (if (should-enqueue? op)
              ; Enqueue compatible ops into the next batch
              (do
                (enqueue! op)
                result)
              ; Flush current batch first
              (let [result (rf result (flush!))]
                (when debug?
                  (info "batch-compatible-operations next result:" result))
                ; Enqueue op into (now empty) batch unless sink stops accepting new values
                (when-not (reduced? result)
                  (enqueue! op))
                result))))))))
  ([test coll]
   (sequence (batch-compatible-operations test) coll)))

(defn batch-commit-last
  "Wraps the last micro op into [:commit nil mop] based on configured probability."
  ([test]
   (let [probability (:batch-commit-probability test 1.0)]
     (fn [rf]
       (let [last (volatile! ::none)]
         (fn
           ([] (rf))
           ([result]
            (let [final @last
                  _ (vreset! last ::none)
                  result (if (identical? final ::none)
                           result
                           ; Push a wrapped final value
                           (let [final (if (< (rand) probability)
                                         [:commit nil final]
                                         final)]
                             (unreduced (rf result final))))]
              (rf result)))
           ([result op]
            (let [prev @last
                  _ (vreset! last ::none)]
              (if (identical? prev ::none)
                (do
                  (vreset! last op)
                  result)
                (let [result (rf result prev)]
                  (when-not (reduced? result)
                    (vreset! last op))
                  result)))))))))
  ([test coll]
   (sequence (batch-commit-last test) coll)))

(defn apply-mop!
  [test tx [f k v :as mop]]
  (case f
    :r [[f k (execute-list-read test tx k)]]
    :append [(do
               (execute-list-append test tx k v)
               mop)]
    :batch (let [results (execute-multi-read-and-append-query test tx (:reads v) (:writes v))
                 readmap (:readmap v)]
             (mapv (fn [[f k _ :as mop]]
                     (case f
                       :r [f k (get results (get readmap k))]
                       :append mop))
                   (:ops v)))
    :commit (do
              (conn/auto-commit! tx)
              (apply-mop! test tx v))))

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
                    ; modified transaction we are going to execute
                    txn' (->> txn
                              (batch-compatible-operations test)
                              (batch-commit-last test)
                              (into []))
                    op' (if (not= txn txn') (assoc op :modified-txn txn') op)
                    ; execute modified transaction and gather results
                    txn'' (->> txn'
                               (mapcat (partial apply-mop! test tx))
                               (into []))
                    op'' (assoc op' :type :ok, :value txn'')]
                op'')))))))

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
