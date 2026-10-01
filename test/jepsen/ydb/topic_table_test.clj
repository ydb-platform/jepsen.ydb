(ns jepsen.ydb.topic-table-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.topic-table :as topic-table]))

(def test-map {:table-key-count 3 :topic-key-count 2})

(deftest topic-key?-test
  (testing "First topic-key-count keys of each block of total-key-count are topic keys"
    (is (true? (topic-table/topic-key? test-map 0)))
    (is (true? (topic-table/topic-key? test-map 1)))
    (is (false? (topic-table/topic-key? test-map 2)))
    (is (false? (topic-table/topic-key? test-map 3)))
    (is (false? (topic-table/topic-key? test-map 4))))
  (testing "Classification is stable across key rotation (fresh-key grows the key space unboundedly)"
    ; total-key-count is 5 here, so keys repeat the same table/topic pattern
    ; every 5 -- a rotated key far past the initial window must classify the
    ; same way as its congruent key within the first window.
    (is (= (topic-table/topic-key? test-map 0) (topic-table/topic-key? test-map 100000)))
    (is (= (topic-table/topic-key? test-map 2) (topic-table/topic-key? test-map 100002)))
    (is (true? (topic-table/topic-key? test-map 100001)))
    (is (false? (topic-table/topic-key? test-map 100004)))))

(deftest simplify-topic-mops-test
  (testing "No topic reads at all: transaction passes through unchanged"
    (testing "any number of topic writes to different keys, plus table reads/writes"
      (is (= [[:append 0 1] [:append 1 2] [:r 2 nil] [:append 3 4] [:r 3 nil]]
             (topic-table/simplify-topic-mops
               test-map [[:append 0 1] [:append 1 2] [:r 2 nil] [:append 3 4] [:r 3 nil]])))))

  (testing "Rule 1: a topic-key read preceded by an append to that same key is dropped"
    (is (= [[:append 0 1]]
           (topic-table/simplify-topic-mops test-map [[:append 0 1] [:r 0 nil]])))
    (testing "later appends to the same key still survive, if no topic read remains at all"
      (is (= [[:append 0 1] [:append 0 2]]
             (topic-table/simplify-topic-mops test-map [[:append 0 1] [:r 0 nil] [:append 0 2]])))))

  (testing "Rule 2: a transaction with a surviving topic-key read collapses to that lone read"
    (testing "a read that precedes the write to the same key survives rule 1, so rule 2 fires and drops the write"
      (is (= [[:r 0 nil]]
             (topic-table/simplify-topic-mops test-map [[:r 0 nil] [:append 0 1]]))))
    (testing "two different topic keys, both read -- only the first survives, nothing else"
      (is (= [[:r 0 nil]]
             (topic-table/simplify-topic-mops test-map [[:r 0 nil] [:r 1 nil]]))))
    (testing "a topic read and a table read -- the table read is dropped regardless of order"
      (is (= [[:r 0 nil]]
             (topic-table/simplify-topic-mops test-map [[:r 2 nil] [:r 0 nil]])))
      (is (= [[:r 0 nil]]
             (topic-table/simplify-topic-mops test-map [[:r 0 nil] [:r 2 nil]]))))
    (testing "writes around a surviving topic read are dropped too, not just other reads"
      (is (= [[:r 0 nil]]
             (topic-table/simplify-topic-mops test-map [[:append 2 1] [:r 0 nil] [:append 1 9]])))))

  (testing "Combined: rule 1 drops the RYOW read, then rule 2 collapses to the one remaining topic read"
    (is (= [[:r 1 nil]]
           (topic-table/simplify-topic-mops
             test-map [[:append 0 5] [:r 0 nil] [:append 0 6] [:r 1 nil]])))))
