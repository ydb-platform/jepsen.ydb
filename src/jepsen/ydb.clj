(ns jepsen.ydb
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [clojure.edn :as edn]
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
            [jepsen.ydb.serializable :as serializable])
  (:import (java.time Duration)
           (java.util ArrayList)
           (com.google.protobuf ByteString)
           (tech.ydb.core StatusCode)
           (tech.ydb.core UnexpectedResultException)
           (tech.ydb.core.grpc GrpcTransport)
           (tech.ydb.table TableClient)
           (tech.ydb.table.query Params)
           (tech.ydb.table.values PrimitiveValue)
           (tech.ydb.table.settings BeginTxSettings)
           (tech.ydb.table.settings CommitTxSettings)
           (tech.ydb.table.settings RollbackTxSettings)
           (tech.ydb.table.settings ExecuteSchemeQuerySettings)
           (tech.ydb.table.transaction Transaction$Mode)
           (tech.ydb.table.transaction TxControl)))

(defn build-transport
  [test node]
  (let [conn-string (str "grpc://" node ":2135?database=" (:db-name test))]
    ;; (info "connecting to" conn-string)
    (-> (GrpcTransport/forConnectionString conn-string)
        .build)))

(defn build-table-client
  [transport]
  (-> (TableClient/newClient transport)
      .build))

(defn open-session
  [table-client]
  (-> table-client
      (.createSession (Duration/ofSeconds 5))
      .join
      .getValue))

(defmacro with-session
  {:clj-kondo/lint-as 'clojure.core/let}
  [[session-name table-client] & body]
  `(with-open [~session-name (open-session ~table-client)]
    ;;  (info "opened session" (.getId ~session-name))
     (let [r# (do ~@body)]
       r#)))

(defprotocol ITransaction
  "Represents a serializable read-write transaction"

  (id [this]
    "Returns the tranasction id")

  (debug-info! [this]
    "Returns accumulated debug info")

  (begin! [this]
    "Explicitly begin the transaction")

  (execute [this query params]
    "Execute a query with the specified params")

  (commit! [this]
    "Commit the transaction, may throw an error when it fails.")

  (rollback! [this]
    "Rollback the transaction, doesn't throw on failure, no-op after commit.")

  (auto-commit! [this]
    "Will cause the next execute to implicitly commit the transaction"))

(defn tx-control-for-execute
  [tx-id commit]
  (-> (if (= tx-id nil)
        (TxControl/serializableRw)
        (TxControl/id tx-id))
      (.setCommitTx commit)))

(defn try-parse-debug-info
  [debug-info]
  (try
    (edn/read-string debug-info)
    (catch Exception e debug-info)))

(deftype Transaction [session
                      ^:unsynchronized-mutable tx-id
                      ^:unsynchronized-mutable auto-commit
                      ^:unsynchronized-mutable debug-info]
  ITransaction
  (id [this]
    tx-id)

  (debug-info! [this]
    (persistent! debug-info))

  (begin! [this]
    (assert (= tx-id nil) "Transaction is already in progress")
    (let [tx (-> session
                 (.beginTransaction Transaction$Mode/SERIALIZABLE_READ_WRITE (BeginTxSettings.))
                 .join
                 .getValue)]
      (set! tx-id (.getId tx))))

  (execute [this query params]
    ;; (info "executing tx query:" query "in tx" (id this) (if auto-commit "with auto commit" nil))
    (let [tx-control (tx-control-for-execute tx-id auto-commit)
          result (-> session
                     (.executeDataQuery query tx-control params)
                     .join
                     .getValue)]
      (if auto-commit
        ; Clear tx-id when we successfully commit implicitly
        (set! tx-id nil)
        ; Remember tx-id when we start a new transaction
        (when (= tx-id nil)
          (set! tx-id (.getTxId result))))
      (when (-> result .hasQueryStats)
        ; FIXME: this is temporary until debug info is passed using dedicated fields (need new java sdk for that)
        (let [ast (-> result .getQueryStats .getQueryAst)]
          (when (. ast startsWith "debug-info:")
            (let [chunk (. ast substring 11)
                  chunk (try-parse-debug-info chunk)]
              (set! debug-info (conj! debug-info chunk))))))
      result))

  (commit! [this]
    (when (not (= tx-id nil))
      (-> session
          (.commitTransaction tx-id (CommitTxSettings.))
          .join
          .expectSuccess)
      (set! tx-id nil)))

  (rollback! [this]
    (when (not (= tx-id nil))
      (-> session
          (.rollbackTransaction tx-id (RollbackTxSettings.))
          .join)
      (set! tx-id nil)))

  (auto-commit! [this]
    (set! auto-commit true)))

(defn open-transaction
  [session]
  (Transaction. session nil false (transient [])))

(defmacro with-transaction
  {:clj-kondo/lint-as 'clojure.core/let}
  [[tx-name session] & body]
  `(let [~tx-name (open-transaction ~session)]
    ;;  (info "opened transaction" (.getId ~tx-name))
     (try
       (let [r# (do ~@body)]
        ;;  (info "commiting transaction" (.getId ~tx-name))
         (commit! ~tx-name)
         r#)
       (catch Exception e#
        ;;  (info "rolling back transaction" (.getId ~tx-name))
         (rollback! ~tx-name)
         (throw e#)))))

(defn execute-scheme-query
  [session query]
  (info "executing scheme query:" query)
  (-> session
      (.executeSchemeQuery query (ExecuteSchemeQuerySettings.))
      .join
      .expectSuccess))

(defn drop-initial-tables
  [test table-client]
  (info "dropping initial tables")
  (with-session [session table-client]
    (let [query (str "DROP TABLE IF EXISTS `" (:db-table test) "`;")]
      (execute-scheme-query session query))))

(defn create-initial-tables
  [test table-client]
  (info "creating initial tables")
  (with-session [session table-client]
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
      (execute-scheme-query session query))))

(defn execute-list-read
  "Executes a list read for the given key k. Works only when the list has at most 1k values."
  [test tx k]
  (let [query (str "DECLARE $key AS Int64;
                    SELECT index, value FROM `" (:db-table test) "`
                    WHERE key = $key
                    ORDER BY index;")
        params (Params/of "$key" (PrimitiveValue/newInt64 k))
        query-result (execute tx query params)
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

(defn assoc-debug-info
  "Associates accumulated debug info from tx with op when present."
  [op tx]
  (let [debug-info (debug-info! tx)]
    (if (> (count debug-info) 0)
      (assoc op :debug-info debug-info)
      op)))

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
    (execute tx query params)))

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
    (auto-commit! tx))
  (apply-mop! test tx mop))

(defmacro with-errors
  [op & body]
  `(try
     ~@body
     (catch UnexpectedResultException e#
       (let [status# (.getStatus e#)
             status-code# (.getCode status#)]
         (cond
           ; Known status codes where operation definitely did not commit
           (= status-code# StatusCode/ABORTED) (assoc ~op :type :fail, :error [:aborted (.toString status#)])
           (= status-code# StatusCode/OVERLOADED) (assoc ~op :type :fail, :error [:overloaded (.toString status#)])
           (= status-code# StatusCode/UNAVAILABLE) (assoc ~op :type :fail, :error [:unavailable (.toString status#)])
           (= status-code# StatusCode/BAD_SESSION) (assoc ~op :type :fail, :error [:bad-session (.toString status#)])
           (= status-code# StatusCode/SESSION_BUSY) (assoc ~op :type :fail, :error [:session-busy (.toString status#)])
           (= status-code# StatusCode/CLIENT_RESOURCE_EXHAUSTED) (assoc ~op :type :fail, :error [:client-resource-exhausted (.toString status#)])
           ; Known status codes where operation may have actually committed
           (= status-code# StatusCode/UNDETERMINED) (assoc ~op :type :info, :error [:undetermined (.toString status#)])
           :else (assoc ~op :type :info, :error [:unexpected-result (.toString status#)]))))))

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(defrecord Client [transport table-client setup?]
  client/Client
  (open! [this test node]
    (let [transport (build-transport test node)
          table-client (build-table-client transport)]
      (assoc this :transport transport :table-client table-client)))

  (setup! [this test]
    (once-per-cluster
     setup?
     (drop-initial-tables test table-client)
     (create-initial-tables test table-client)))

  (invoke! [_ test op]
    ; TODO: handle known errors!
    ;; (info "processing op:" op)
    (with-errors op
      (with-session [session table-client]
        (with-transaction [tx session]
          (let [txn (:value op)
                txn' (mapv (partial apply-mop-with-auto-commit-last! test tx)
                           txn
                           (iterate inc 0)
                           (repeat (count txn)))
                op' (assoc op :type :ok, :value txn')
                op' (assoc-debug-info op' tx)]
            op')))))

  (teardown! [this test])

  (close! [this test]
    (.close table-client)
    (.close transport)))

(defn append-workload
  [opts]
  (-> (serializable/append-test (assoc (select-keys opts [:key-count
                                                          :min-txn-length
                                                          :max-txn-length
                                                          :max-writes-per-key])
                                       :consistency-models [:serializable]))
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
                   (cli/serve-cmd))
            args))
