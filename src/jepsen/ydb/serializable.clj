(ns jepsen.ydb.serializable
  (:require [elle.core :as elle]
            [elle.list-append :as a]
            [jepsen.history :as h]
            [jepsen.checker :as checker]
            [jepsen.tests.cycle.append :as append]
            [bifurcan-clj [core :as b]]
            [elle.graph :as g]
            [elle.rels :as rels :refer [ww wr rw process realtime]]
            [dom-top.core :refer [loopr]])
  (:import (io.lacuna.bifurcan ISet
                               Set)
           (jepsen.history Op)
           (elle.core RealtimeExplainer)))

(defn mop-key
  [[f k v :as mop]]
  (case f
    :r k
    :append k))

(defn op-keys
  [op]
  (apply hash-set (map mop-key (:value op))))

(defn link-per-key-oks
  [alloks g op op']
  (reduce
   ; Iterate for each affected key k in op
   ; When a given key has oks set we link them to completion op'
   (fn [g k]
     (let [oks (get alloks k)]
       (if (nil? oks)
         g
         (g/link-all-to g oks op' realtime))))
   g (op-keys op)))

(defn ^ISet oks-without-implied
  [^ISet oks g op]
  (if (nil? oks)
    ; Create a new empty set
    (.linear (Set.))
    ; Find and remove implied operations
    (let [implied (g/in g op)]
      (if implied
        (.difference oks implied)
        oks))))

(defn add-per-key-oks
  [alloks g op]
  (reduce
   ; Iterate for each affected key k in op
   ; Adds op to oks buffer for each key, removing implied ops
   (fn [alloks k]
     (let [oks (get alloks k)
           oks (oks-without-implied oks g op)
           oks (.add oks op)]
       (assoc alloks k oks)))
   alloks (op-keys op)))

(defn ydb-realtime-graph
  "A modification of realtime-graph from elle, where only per-key realtime
   edges are added. This corresponds to per-key linearizability."
  [history]
  (loopr [alloks {} ; Our buffer of completed ops for each key
          g      (b/linear (g/op-digraph))] ; Our order graph
         [op history :via :reduce]
         (case (:type op)
           ; A new operation begins! Link every completed op to this one's
           ; completion. Note that we generate edges here regardless of whether
           ; this op will fail or crash--we might not actually NEED edges to
           ; failures, but I don't think they'll hurt. We *do* need edges to
           ; crashed ops, because they may complete later on.
           :invoke ; NB: we might get a partial history without completions
           (if-let [op' (h/completion history op)]
             (recur alloks (link-per-key-oks alloks g op op'))
             (recur alloks g))

           ; An operation has completed. Add it to the oks buffer, and remove
           ; oks that this ok implies must have completed.
           :ok
           (let [alloks (add-per-key-oks alloks g op)]
             (recur alloks g))
           ; An operation that failed doesn't affect anything--we don't generate
           ; dependencies on failed transactions because they didn't happen. I
           ; mean we COULD, but it doesn't seem useful.
           :fail (recur alloks g)
           ; Crashed operations, likewise: nothing can follow a crashed op, so
           ; we don't need to add them to the ok set.
           :info (recur alloks g))
         ; All done!
         {:graph     (b/forked g)
          :explainer (RealtimeExplainer. history)}))

(defn ydb-check-opts
  "Modified check opts for YDB consistency model."
  [opts]
  (let [requested-models (:consistency-models opts)
        ; YDB is not strict serializable, but we use modified realtime graph
        ; to relax some realtime requirements and then check as if it was
        ; strict serializable.
        checked-models (cons :strict-serializable requested-models)]
    (assoc opts :consistency-models checked-models)))

(defn append-check
  "Checks history using YDB consistency model."
  ([history]
   (append-check {} history))
  ([opts history]
   (with-redefs [elle/realtime-graph ydb-realtime-graph]
     (a/check (ydb-check-opts opts) history))))

(defn ydb-checker
  "Wraps an existing jepsen checker with a modified YDB realtime graph."
  [wrapped]
  (reify checker/Checker
    (check [this test history checker-opts]
      (with-redefs [elle/realtime-graph ydb-realtime-graph]
        (checker/check wrapped test history checker-opts)))))

(defn append-test
  "A partial test for YDB consistency model."
  [opts]
  (let [opts (ydb-check-opts opts)
        test (append/test opts)]
    (assoc test :checker (ydb-checker (:checker test)))))
