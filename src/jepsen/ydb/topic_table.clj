(ns jepsen.ydb.topic-table
  "Mixes table and topic operations inside single YDB transactions to catch
   atomicity violations between the SQL API and the Topic API (see issue
   ydb-platform/jepsen.ydb#30). Table-key micro-ops are unrestricted, exactly
   like jepsen.ydb.append, in any transaction that doesn't read a topic key.
   A transaction that reads a topic key is heavily restricted (see
   simplify-topic-mops for the exact rules and why): it collapses down to
   that one lone read, nothing else -- no other reads, no writes at all,
   topic or table. Topic own-writes are invisible within the same
   transaction, and a topic replay read is never attached to the
   transaction (it can't be snapshot-pinned regardless), so it always runs
   strictly before the transaction's actual commit point, at a different
   moment than anything else in that same transaction takes effect. An
   unrestricted mix would surface false-positive read-your-own-writes /
   repeatable-read / torn-read / non-atomic-operation anomalies that are
   inherent to topic semantics and to this timing gap, not atomicity bugs
   (this is exactly what happened in the topics-only POC on branch
   topic-poc, and in real cluster runs and code review of earlier,
   insufficiently-restricted versions of this workload). Writes are always
   unrestricted in a transaction that doesn't read a topic key -- they only
   take effect atomically at commit. Restricting reads this way makes a
   topic read behave, from the checker's point of view, like an isolated
   observation rather than part of a torn multi-key transaction -- so the
   unmodified Elle list-append checker (via jepsen.tests.cycle.append) can
   be reused across the combined table+topic keyspace without a
   topic-specific checker, and cross-transaction atomicity is still
   detected via realtime ordering (see jepsen.ydb.serializable) rather than
   via same-transaction co-reads."
  (:require [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ydb.append :as table]
            [jepsen.ydb.conn :as conn]
            [jepsen.ydb.debug-info :as debug-info]
            [jepsen.ydb.serializable :as ydb-serializable]
            [jepsen.ydb.topic :as topic])
  (:import (java.util.concurrent TimeUnit)))

(defmacro once-per-cluster
  [atomic-bool & body]
  `(locking ~atomic-bool
     (when (compare-and-set! ~atomic-bool false true) ~@body)))

(def touched-topic-keys
  "Every topic key any transaction has ever attempted an :r or :append on,
   across the whole test run. Used to build a final read sweep (see
   final-reads-gen) -- jepsen 0.3.10 (pinned in project.clj) predates
   jepsen.tests.cycle.core/final-gen and max-key-tracker (added in a later
   Jepsen release), so jepsen.tests.cycle.append/test provides no built-in
   guarantee that every key gets read again before the test ends. That
   matters more here than for table-only workloads: simplify-topic-mops
   restricts reads of topic keys much more heavily than table keys (see its
   docstring), so a given topic key's :append(s) are comparatively unlikely
   to ever be read back again by the ordinary random generator alone,
   without an explicit final sweep."
  (atom #{}))

(defn new-final-reads-gen
  "Builds a fresh generator (an arity-0 function; see jepsen.generator's
   Fn/AFunction handling of plain functions) that reads every topic key ever
   touched, once, one key per :txn. Evaluated lazily -- only invoked once the
   main phase has finished and touched-topic-keys is fully populated, via
   ydb-test's existing (:final-generator workload) handling (already wired
   up for kafka-topic; --kafka-final-time-limit governs its time budget for
   any workload, including this one).

   One key per :txn, not batched: this generator is wired directly as
   :final-generator and, unlike the main generator, never passes through
   gen/map/simplify-topic-mops (ydb-test's final-generator handling just
   does (gen/clients workload-final-gen), no simplification step) -- so
   nothing else enforces simplify-topic-mops's rule 2 here. Batching several
   topic reads into one :txn would recreate exactly the torn-read problem
   rule 2 exists to avoid: execute-topic-read! isn't snapshot-pinned, so the
   reads in one batch each run at a different real moment, and if some other
   transaction commits in the gap between the batch's first and last read,
   Elle sees our one :ok completion as a single atomic transaction that
   observed an impossible torn combination of before- and after-states.
   Running this generator's ops through simplify-topic-mops instead of
   splitting one-key-per-txn isn't an option either -- rule 2 would keep only
   the first key of each batch and silently drop the rest, defeating the
   point of a *complete* sweep.

   The returned function is one-shot (guarded by emitted?): jepsen's Fn
   generator wrapper calls an arity-0 generator function again once the
   sequence it returned is exhausted, falling back to treating a fresh call
   as a fresh generator -- without the guard, this would call us forever,
   endlessly re-reading the same keys until the time limit cuts it off,
   rather than reading each key once and finishing."
  []
  (let [emitted? (atom false)]
    (fn []
      (when (compare-and-set! emitted? false true)
        (->> @touched-topic-keys
             sort
             (map (fn [k] {:type :invoke, :f :txn, :value [[:r k nil]]})))))))

(defn total-key-count
  "The combined table+topic key space size, used both to configure the
   underlying append/test generator's :key-count and to classify keys here.
   Kept as a single function so the two can never drift apart."
  [test]
  (+ (:table-key-count test) (:topic-key-count test)))

(defn topic-key?
  "Classifies a (potentially unbounded, ever-rotating) Jepsen key as
   table or topic, deterministically by its value mod the total key count --
   not by an initial [0, topic-key-count) range -- since elle.txn/fresh-key
   retires and replaces keys as the test runs, so the key space grows well
   past the initial window over a test's lifetime."
  [test k]
  (< (mod (long k) (total-key-count test)) (:topic-key-count test)))

(defn simplify-topic-mops
  "Given a transaction's micro-ops, returns a simplified vector safe to run
   against topics that aren't snapshot-isolated within a transaction, and
   whose reads aren't pinned to the transaction's commit point either. Two
   rules, applied in order:

   1. Drops a topic-key read if that same key was already appended to
      earlier in this transaction. Topic own-writes aren't visible within
      the same transaction, so a read immediately reflecting them would
      look like an internal-consistency violation to Elle (this is the
      original per-key read-your-own-writes/repeatable-read concern from
      the abandoned topics-only POC on branch topic-poc). A read that
      precedes the write to the same key is unaffected -- it isn't
      expected to see a write that, in program order, hasn't happened yet.

   2. After (1), if any topic-key read remains, the ENTIRE transaction
      collapses to just that one read (the first survivor) -- every other
      mop is dropped, reads AND writes alike. execute-topic-read! is never
      attached to the transaction (a topic replay read can't be
      snapshot-pinned regardless -- see its docstring), so it executes and
      returns at whatever real time apply-mop! happens to reach it, which
      is always strictly BEFORE the transaction's actual commit (commit
      only happens once every mop, including this read, has already run).
      Mop order inside the transaction doesn't change this -- the read is
      always earlier in real time than the commit. So even though a write
      in the same transaction only takes effect atomically AT commit, that
      commit point is a different, later moment than the read's. If some
      other transaction T atomically commits writes to both the key our
      read touched (A) and some key our transaction also writes (B)
      somewhere in that gap, our transaction legitimately doesn't see T's
      effect on A (the read ran before T committed) while its own write to
      B lands, in commit order, after T's write to B -- from the outside,
      that looks like \"before T\" via A and \"after T\" via B at once, a
      cycle no real atomic transaction could produce. Elle would correctly
      flag that as impossible, but the actual cause would be our own
      non-instantaneous mixed operation, not a real YDB bug. Collapsing to
      a lone read removes anything else in the transaction whose effective
      timing could disagree with when the read actually ran. Transactions
      with no topic-key read at all are returned unchanged and stay fully
      unrestricted (any number of table/topic writes, any number of table
      reads) -- all their effects, reads included, are then either
      properly snapshot-consistent (table reads, under YDB's own
      SERIALIZABLE_RW guarantee) or only take hold atomically at commit
      (writes), so there's nothing for this problem to apply to.

   Applied at the GENERATOR level (see workload, via gen/map), not
   client-side in invoke! -- Elle's checker (elle.txn/intermediate-write-
   indices, used for G1b/intermediate-read detection, and potentially other
   analyses) reads straight from the raw history, including :invoke entries,
   which Jepsen always logs with the exact value the generator produced,
   before any client ever sees it. If we simplified only inside invoke! (as
   an earlier version of this code did), the :invoke entry would still show
   the original, never-executed extra touches, and Elle would treat those
   phantom writes/reads as real. Simplifying at the generator means the
   :invoke entry Jepsen logs already matches what actually runs, so there's
   nothing for the checker to misread."
  [test mops]
  (let [written (volatile! #{})
        rule-1 (vec (keep (fn [[f k _ :as mop]]
                             (cond
                               (= f :append)
                               (do (when (topic-key? test k) (vswap! written conj k))
                                   mop)

                               (and (= f :r) (topic-key? test k) (contains? @written k))
                               nil

                               :else mop))
                           mops))
        first-topic-read (->> rule-1
                               (filter (fn [[f k _]] (and (= f :r) (topic-key? test k))))
                               first)]
    (if (nil? first-topic-read)
      rule-1
      [first-topic-read])))

(defn apply-mop!
  [test tx topic-client writers [f k v :as mop]]
  (if (topic-key? test k)
    (do
      (swap! touched-topic-keys conj k)
      (case f
        :r [[f k (topic/execute-topic-read! test topic-client k)]]
        :append [(do (topic/execute-topic-append! test tx topic-client writers k v) mop)]))
    (case f
      :r [[f k (table/execute-list-read test tx k)]]
      :append [(do (table/execute-list-append test tx nil k v) mop)])))

(defrecord Client [transport query-client topic-client writers ballast setup?]
  client/Client
  (open! [this test node]
    (let [transport (conn/open-transport test node)
          query-client (conn/open-query-client transport)
          topic-client (conn/open-topic-client transport)]
      ; writers must be a fresh, per-worker atom, NOT the prototype's --
      ; each cached AsyncWriter is bound to the specific topic-client that
      ; created it (via .createAsyncWriter), and every worker gets its own
      ; topic-client above. Leaving writers shared (the prototype Client's
      ; atom, copied by reference through assoc) let one worker's writer,
      ; tied to its own connection, end up cached and reused by a different
      ; worker -- if the owning worker's client later closed, the other
      ; worker would hit "Writer is already stopped" trying to reuse it.
      ; It also raced: concurrent workers sharing one atom could both miss
      ; the cache for the same partition and overwrite each other's entry
      ; in get-writer!'s check-then-act. Since each worker's own atom is
      ; only ever touched by that worker's single dedicated thread, this
      ; also removes the race, not just the cross-worker reuse.
      (assoc this :transport transport :query-client query-client
                  :topic-client topic-client :writers (atom {}))))

  (setup! [this test]
    (once-per-cluster
     setup?
     (reset! touched-topic-keys #{})
     (table/drop-initial-tables test query-client)
     (table/create-initial-tables test query-client)
     (topic/drop-initial-topic test query-client)
     (topic/create-initial-topic test query-client)))

  (invoke! [_ test op]
    (table/with-ballast ballast
      (debug-info/with-debug-info
        (conn/with-errors op
          (conn/with-session [session query-client]
            (conn/with-transaction [tx [session (:model test)]]
              (let [txn (:value op)
                    ; txn is already simplified by the generator (see
                    ; workload/simplify-topic-mops) -- nothing to do here but
                    ; execute it.
                    txn' (->> txn
                              (mapcat (partial apply-mop! test tx topic-client writers))
                              (into []))]
                (assoc op :type :ok, :value txn'))))))))

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
  (Client. nil nil nil (atom {}) (table/new-ballast (:ballast-size opts)) (atom false)))

(defn new-simplify-topic-mops-in-op
  "Builds a single-arity fn (NOT via partial/clojure.core -- see below) that
   transforms a generated op by simplifying its :value -- see
   simplify-topic-mops. Passed to gen/map so the transaction Jepsen logs at
   :invoke time is already what will actually run.

   Must be a plain (fn [op] ...) closure, not (partial f opts): gen/map
   picks which arity to call f with (1 or 3 args) by reflecting on
   (.getDeclaredMethods (class f)) for the highest-arity `invoke` method it
   finds. clojure.core/partial's returned function genuinely implements
   invoke at several arities (0, 1, 2, 3, & more), all of which just forward
   to the wrapped function with the fixed args prepended -- so reflection
   sees a spurious 3-arg invoke and gen/map calls it as (f op test ctx),
   which prepends opts too and calls the real 2-arity fn with 4 args,
   throwing ArityException. A plain (fn [op] ...) only ever has a genuine
   1-arg invoke, so reflection reports arity 1 correctly."
  [opts]
  (fn [op]
    (if (= :txn (:f op))
      (update op :value (partial simplify-topic-mops opts))
      op)))

(defn workload
  [opts]
  (-> (ydb-serializable/wrap-test
       (update (append/test (assoc (select-keys opts [:min-txn-length :max-txn-length :max-writes-per-key])
                                   :key-count (+ (:table-key-count opts) (:topic-key-count opts))
                                   :consistency-models [(:model opts)]))
               :generator (partial gen/map (new-simplify-topic-mops-in-op opts))))
      (assoc :client (new-client opts)
             :final-generator (new-final-reads-gen))))
