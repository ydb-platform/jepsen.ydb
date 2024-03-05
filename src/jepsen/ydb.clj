(ns jepsen.ydb
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.os :as os]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.tests :as tests]
            [jepsen.tests.cycle.append :as append]
            [jepsen.control.util :as cu]
            [jepsen.ydb.cli-clean :as cli-clean]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable])
  (:import (java.util ArrayList)
           (com.google.protobuf ByteString)
           (tech.ydb.table.query Params)
           (tech.ydb.table.values PrimitiveValue)))

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
  "Executes a list read for the given key k. Works only when the list has at most 1k values."
  [test tx k]
  (let [query (str "DECLARE $key AS Int64;
                    SELECT index, value FROM `" (:db-table test) "`
                    WHERE key = $key
                    ORDER BY index;")
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        query-result (conn/execute! tx query params)
        rs (. query-result getResultSet 0)
        result (ArrayList.)]
    (assert (not (.isTruncated rs)) "List read result was truncated")
    (while (. rs next)
      (let [index (-> rs (.getColumn 0) .getInt64)
            value (-> rs (.getColumn 1) .getInt64)
            expectedIndex (.size result)]
        (assert (= index expectedIndex) "List indexes are not ordered correctly")
        (. result add value)))
    (if (> (.size result) 0)
      (vec result)
      nil)))

(def ballast-obj (atom nil))

(defn ballast-set-size!
  [size]
  (reset! ballast-obj (ByteString/copyFromUtf8 (.repeat "x" size))))

(defn ballast
  []
  (deref ballast-obj))

(defn execute-list-append
  [test tx k v]
  (let [query (str "DECLARE $key AS Int64;
                    DECLARE $value AS Int64;
                    DECLARE $ballast AS Bytes;
                    $next_index = (SELECT COALESCE(MAX(index) + 1, 0) FROM `" (:db-table test) "` WHERE key = $key);
                    UPSERT INTO `" (:db-table test) "` (key, index, value, ballast) VALUES ($key, $next_index, $value, $ballast);")
        params (Params/of "$key" (PrimitiveValue/newInt64 k)
                          "$value" (PrimitiveValue/newInt64 v)
                          "$ballast" (PrimitiveValue/newBytes (ballast)))]
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

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defrecord Client [transport table-client setup?]
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
    ; TODO: handle known errors!
    ;; (info "processing op:" op)
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
              op'))))))

  (teardown! [this test])

  (close! [this test]
    (.close table-client)
    (.close transport)))

(defn append-workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (append/test (assoc (select-keys opts [:key-count
                                              :min-txn-length
                                              :max-txn-length
                                              :max-writes-per-key])
                           :consistency-models [:ydb-serializable])))
      (assoc :client (Client. nil nil (atom false)))))

(defn ydb-test
  "Tests YDB"
  [opts]
  (ballast-set-size! (:ballast-size opts))
  (let [workload (append-workload opts)
        db       db/noop
        os       os/noop
        packages (nc/nemesis-packages
                  {:db        db
                   :nodes     (:nodes opts)
                   :faults    (:nemesis opts)
                   :partition {:targets [:one]}
                   :pause     {:targets [:one]}
                   :kill      {:targets [:one :all]}
                   :interval  (:nemesis-interval opts)})
        ; The default nemesis-package will compose all available packages, even
        ; those that are not enabled. This includes bitflip, which downloads
        ; and installs an archive from github.com, which doesn't work in
        ; restricted networks. Filter only those that have a generator.
        needed-packages (filter
                         (fn [x] (not (= (:generator x) nil)))
                         packages)
        nemesis (nc/compose-packages needed-packages)]

    (merge tests/noop-test
           opts
           {:name "ydb"
            :db db
            :os os
            :client (:client workload)
            :nemesis (:nemesis nemesis)
            :checker (checker/compose
                      {:perf (checker/perf
                              {:nemeses (:perf nemesis)})
                       :clock (checker/clock-plot)
                       :stats (checker/stats)
                       :exceptions (checker/unhandled-exceptions)
                       :workload (:checker workload)})
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def cli-opts
  "Command line options"
  [[nil "--db-name DBNAME" "YDB database name."
    :default "/local"]

   [nil "--db-port NUM" "YDB database port."
    :default 2135
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--db-table NAME" "YDB table name to use for testing."
    :default "jepsen_test"]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--ballast-size NUM" "Number of ballast bytes added to values"
    :default 1000
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  16
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]])

(defn -main
  "Handles command line arguments."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn ydb-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd)
                   (cli-clean/clean-valid-cmd))
            args))
