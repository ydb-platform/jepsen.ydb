(ns jepsen.ydb.conn
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.ydb.debug-info :as debug-info])
  (:import (java.time Duration)
           (tech.ydb.common.transaction TxMode)
           (tech.ydb.core StatusCode)
           (tech.ydb.core UnexpectedResultException)
           (tech.ydb.core.grpc GrpcTransport)
           (tech.ydb.query QueryClient)
           (tech.ydb.query.settings ExecuteQuerySettings)
           (tech.ydb.query.tools QueryReader)
           (tech.ydb.table.query Params)))

(defn open-transport
  "Opens a new grpc transport using the specified test and node"
  [test node]
  (let [conn-string (str "grpc://" node ":" (:db-port test) "?database=" (:db-name test))]
    ;(info "connecting to" conn-string)
    (-> (GrpcTransport/forConnectionString conn-string)
        .build)))

(defn open-query-client
  "Opens a new query client using the specified transport"
  [transport]
  (-> (QueryClient/newClient transport)
      .build))

(defn open-session
  "Opens a YDB session using the specified table client"
  [query-client]
  (-> query-client
      (.createSession (Duration/ofSeconds 5))
      .join
      .getValue))

(defmacro with-session
  "Wraps code block, opening session before the start and closing it before leaving.

   (with-session [session query-client]
     ... use session)"
  {:clj-kondo/lint-as 'clojure.core/let}
  [[session-name query-client] & body]
  `(with-open [~session-name (open-session ~query-client)]
     ;(info "opened session" (.getId ~session-name))
     (let [r# (do ~@body)]
       r#)))

(defprotocol ITransaction
  "Represents a serializable read-write transaction"

  (current-tx-id [this]
    "Returns the current transaction id, or nil when no transaction is open.")

  (begin! [this]
    "Explicitly begin the transaction, transaction must not be open yet.")

  (auto-commit! [this]
    "Will cause the next execute! to atomically commit the transaction.")

  (execute! [this query params]
    "Execute a query with the specified params, opening a new transaction when necessary.")

  (commit! [this]
    "Explicitly commit the transaction. May throw on failure. No-op when transaction is not open.")

  (rollback! [this]
    "Explicitly rollback the transaction. Doesn't throw on failure. No-op when transaction is not open."))

(defn handle-debug-info
  "Handle debug info in the result (when present)"
  [result]
  (when (-> result .getQueryInfo .hasStats)
    ; When present debug info is temporarily passed in the ast field with a special prefix
    (let [ast (-> result .getQueryInfo .getStats .getQueryAst)
          debug-info-prefix "debug-info:"]
      (when (. ast startsWith debug-info-prefix)
        (let [chunk (. ast substring (.length debug-info-prefix))
              chunk (debug-info/try-parse-debug-info chunk)]
          (debug-info/add-debug-info chunk))))))

(def commit-via-select-1? true)

(defn model-to-tx-mode
  "Converts model to transaction mode"
  [model]
  (case model
    :ydb-serializable TxMode/SERIALIZABLE_RW
    :snapshot-isolation TxMode/SNAPSHOT_RW))

(deftype Transaction [session mode
                      ^:unsynchronized-mutable tx
                      ^:unsynchronized-mutable auto-commit]
  ITransaction
  (current-tx-id [this]
    (if (not (= tx nil))
      (.getId tx)
      nil))

  (begin! [this]
    (assert (= tx nil) "Transaction is already in progress")
    (assert (= auto-commit false) "Cannot begin new transaction after the call to auto-commit!")
    (set! tx (-> session
                 (.beginTransaction mode)
                 .join
                 .getValue)))

  (auto-commit! [this]
    (set! auto-commit true))

  (execute! [this query params]
    (when (and (not auto-commit) (= tx nil))
      (set! tx (-> session (.createNewTransaction mode))))
    ;; (info "executing tx query:" query "in tx" (current-tx-id this) (if auto-commit "with auto commit" ""))
    (let [
          stream (if (= tx nil)
                       (-> session (.createQuery query mode params))
                       (-> tx (.createQuery query auto-commit params (-> (ExecuteQuerySettings/newBuilder) .build))))
          _ (set! auto-commit false)
          result (-> (QueryReader/readFrom stream) .join .getValue)]
      (when (and (not (= tx nil)) (not (-> tx .isActive)))
        ; Clear tx when we successfully commit implicitly
        (set! tx nil))
      (handle-debug-info result)
      result))

  (commit! [this]
    (assert (= auto-commit false) "Cannot commit transaction after the call to auto-commit!")
    (when (not (= tx nil))
      (if commit-via-select-1?
        (do
          ; Perform SELECT 1 with auto commit to gather debug-info
          (set! auto-commit true)
          (execute! this "SELECT 1" (Params/empty))
          nil)
        (do
          (-> tx
              .commit
              .join
              .getStatus
              .expectSuccess)
          (set! tx nil)))))

  (rollback! [this]
    (set! auto-commit false)
    (when (not (= tx nil))
      (-> tx .rollback .join)
      (set! tx nil))))

(defn open-transaction
  "Returns a new Transaction object using the specified session"
  [session model]
  (Transaction. session (model-to-tx-mode model) nil false))

(defmacro with-transaction
  "Wraps a code block with a Transaction object, which will be committed on success or rolled back on exception.

   (with-transaction [tx [session model]]
     ... use tx object)"
  {:clj-kondo/lint-as 'clojure.core/let}
  [[tx-name [session model]] & body]
  `(let [~tx-name (open-transaction ~session ~model)]
     ;(info "opened transaction" (.getId ~tx-name))
     (try
       (let [r# (do ~@body)]
         ;(info "commiting transaction" (.getId ~tx-name))
         (commit! ~tx-name)
         r#)
       (catch Exception e#
         ;(info "rolling back transaction" (.getId ~tx-name))
         (rollback! ~tx-name)
         (throw e#)))))

(defn execute-scheme!
  "Executes a scheme query using the specified session."
  [session query]
  ;(info "executing scheme query:" query)
  (-> session
      (.createQuery query TxMode/NONE)
      .execute
      .join
      .getStatus
      .expectSuccess))

(defmacro with-errors
  "Takes an op and a code block, will assoc :type :fail or :type :info on known exceptions."
  [op & body]
  `(try
     ~@body
     (catch UnexpectedResultException e#
       ;(info "got exception" e#)
       (let [status# (.getStatus e#)
             status-code# (.getCode status#)]
         ; TODO: assoc partial results for individual mops
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
           ; For other exceptions we assume we don't know whether it committed or not
           :else (assoc ~op :type :info, :error [:unexpected-result (.toString status#)]))))))
