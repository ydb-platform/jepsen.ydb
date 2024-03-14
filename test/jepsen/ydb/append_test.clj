(ns jepsen.ydb.append-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.append :as append])
  (:import (jepsen.ydb.append OperationBatch)))

(defn- make-batch
  [test ops & xforms]
  (into [] (apply comp (append/batch-compatible-operations test) xforms) ops))

(deftest make-multi-read-and-append-params
  (testing "A single read"
    (append/make-multi-read-and-append-params [1] []))
  (testing "A single write"
    (append/make-multi-read-and-append-params [] [[1 1]])))

(deftest batch-compatible-operations
  (testing "Empty list of operations"
    (is (= [] (make-batch {} []))))
  (testing "A single read operation"
    (let [ops [[:r 1 nil]]
          expected [[:batch #{1} (OperationBatch. ops [1] [] {1 0})]]]
      (is (= expected (make-batch {} ops)))))
  (testing "A single append operation"
    (let [ops [[:append 1 42]]
          expected [[:batch #{1} (OperationBatch. ops [] [[1 42]] {})]]]
      (is (= expected (make-batch {} ops)))))
  (testing "Incompatible append then read"
    (let [ops [[:append 1 42] [:r 1 nil]]
          expected [[:batch #{1} (OperationBatch. [[:append 1 42]] [] [[1 42]] {})]
                    [:batch #{1} (OperationBatch. [[:r 1 nil]] [1] [] {1 0})]]]
      (is (= expected (make-batch {} ops)))))
  (testing "Incompatible append then read, take 2"
    (let [ops [[:append 1 42] [:r 1 nil]]
          expected [[:batch #{1} (OperationBatch. [[:append 1 42]] [] [[1 42]] {})]
                    [:batch #{1} (OperationBatch. [[:r 1 nil]] [1] [] {1 0})]]]
      (is (= expected (make-batch {} ops (take 2))))))
  (testing "Incompatible append then read, take 1"
    (let [ops [[:append 1 42] [:r 1 nil]]
          expected [[:batch #{1} (OperationBatch. [[:append 1 42]] [] [[1 42]] {})]]]
      (is (= expected (make-batch {} ops (take 1))))))
  (testing "Incompatible append then read, take 1 via ->>"
    (let [ops [[:append 1 42] [:r 1 nil]]
          expected [[:batch #{1} (OperationBatch. [[:append 1 42]] [] [[1 42]] {})]]]
      (is (= expected (into [] (->> ops
                                    (append/batch-compatible-operations {})
                                    (take 1)))))))
  (testing "Incompatible append then read, take 0"
    (let [ops [[:append 1 42] [:r 1 nil]]
          expected []]
      (is (= expected (make-batch {} ops (take 0))))))
  (testing "Incompatible append then read, original ops"
    (let [ops [[:append 1 42] [:r 1 nil]]
          expected ops]
      (is (= expected (make-batch {:batch-single-ops false} ops)))))
  (testing "Incompatible append then multiple reads, original append op"
    (let [ops [[:append 1 42] [:r 1 nil] [:r 2 nil]]
          expected [[:append 1 42]
                    [:batch #{1 2} (OperationBatch. [[:r 1 nil] [:r 2 nil]] [1 2] [] {1 0 2 1})]]]
      (is (= expected (make-batch {:batch-single-ops false} ops)))))
  (testing "Compatible read then append"
    (let [ops [[:r 1 nil] [:append 1 42]]
          expected [[:batch #{1} (OperationBatch. ops [1] [[1 42]] {1 0})]]]
      (is (= expected (make-batch {} ops)))))
  (testing "Multiple compatible reads combined"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 1 nil] [:r 3 nil]]
          expected [[:batch #{1 2} (OperationBatch. [[:r 1 nil] [:r 2 nil]] [1 2] [] {1 0 2 1})]
                    [:batch #{1 3} (OperationBatch. [[:r 1 nil] [:r 3 nil]] [1 3] [] {1 0 3 1})]]]
      (is (= expected (make-batch {} ops)))))
  (testing "Multiple compatible reads not combined due to low probability"
    (let [ops [[:r 1 nil] [:r 2 nil]]
          expected [[:batch #{1} (OperationBatch. [[:r 1 nil]] [1] [] {1 0})]
                    [:batch #{2} (OperationBatch. [[:r 2 nil]] [2] [] {2 0})]]]
      (is (= expected (make-batch {:batch-ops-probability 0.0} ops))))))

(defn- with-commit-last
  [test ops & xforms]
  (into [] (apply comp (append/batch-commit-last test) xforms) ops))

(deftest batch-commit-last
  (testing "Empty list of operations"
    (is (= [] (with-commit-last {} []))))
  (testing "A single operation"
    (let [ops [[:r 1 nil]]
          expected [[:commit nil [:r 1 nil]]]]
      (is (= expected (with-commit-last {} ops)))))
  (testing "Two operations"
    (let [ops [[:r 1 nil] [:r 2 nil]]
          expected [[:r 1 nil] [:commit nil [:r 2 nil]]]]
      (is (= expected (with-commit-last {} ops)))))
  (testing "Multiple operations"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:commit nil [:r 4 nil]]]]
      (is (= expected (with-commit-last {} ops)))))
  (testing "Multiple operations, take 3"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected [[:r 1 nil] [:r 2 nil] [:r 3 nil]]]
      (is (= expected (with-commit-last {} ops (take 3))))))
  (testing "Multiple operations, take 2"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected [[:r 1 nil] [:r 2 nil]]]
      (is (= expected (with-commit-last {} ops (take 2))))))
  (testing "Multiple operations, no :commit due to low probability"
    (let [ops [[:r 1 nil] [:r 2 nil] [:r 3 nil] [:r 4 nil]]
          expected ops]
      (is (= expected (with-commit-last {:batch-commit-probability 0.0} ops))))))
