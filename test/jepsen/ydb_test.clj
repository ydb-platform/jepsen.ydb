(ns jepsen.ydb-test
  (:require [clojure.test :refer :all]
            [jepsen.ydb :refer :all]
            [jepsen.history :as h]
            [jepsen.ydb.serializable :as serializable]))

(deftest per-key-realtime-reorder-detected
  (testing "Per key realtime reorder detected"
    (let [h (h/history
             [{:process 0, :time 1000, :type :invoke, :f :txn, :value [[:append 1 1] [:append 3 1]]}
              {:process 1, :time 1010, :type :invoke, :f :txn, :value [[:r 2 nil] [:append 3 2]]}
              {:process 1, :time 1020, :type :ok,     :f :txn, :value [[:r 2 nil] [:append 3 2]]}
              {:process 2, :time 1030, :type :invoke, :f :txn, :value [[:r 1 nil] [:r 2 nil]]}
              {:process 2, :time 1040, :type :ok,     :f :txn, :value [[:r 1 nil] [:r 2 nil]]}
              {:process 1, :time 1050, :type :invoke, :f :txn, :value [[:r 2 nil] [:append 3 3]]}
              {:process 1, :time 1060, :type :ok,     :f :txn, :value [[:r 2 nil] [:append 3 3]]}
              {:process 0, :time 1070, :type :ok,     :f :txn, :value [[:append 1 1] [:append 3 1]]}
              {:process 0, :time 1080, :type :invoke, :f :txn, :value [[:r 3 nil]]}
              {:process 0, :time 1090, :type :ok,     :f :txn, :value [[:r 3 [1 2 3]]]}])
          r (serializable/append-check h)]
      (is (not (:valid? r))))))

(deftest per-key-realtime-no-reorder
  (testing "Per key realtime no reorder"
    (let [h (h/history
             [{:process 0, :time 1000, :type :invoke, :f :txn, :value [[:append 1 1] [:append 3 1]]}
              {:process 1, :time 1010, :type :invoke, :f :txn, :value [[:r 2 nil] [:append 3 2]]}
              {:process 1, :time 1020, :type :ok,     :f :txn, :value [[:r 2 nil] [:append 3 2]]}
              {:process 2, :time 1030, :type :invoke, :f :txn, :value [[:r 1 nil] [:r 2 nil]]}
              {:process 2, :time 1040, :type :ok,     :f :txn, :value [[:r 1 [1]] [:r 2 nil]]}
              {:process 1, :time 1050, :type :invoke, :f :txn, :value [[:r 2 nil] [:append 3 3]]}
              {:process 1, :time 1060, :type :ok,     :f :txn, :value [[:r 2 nil] [:append 3 3]]}
              {:process 0, :time 1070, :type :ok,     :f :txn, :value [[:append 1 1] [:append 3 1]]}
              {:process 0, :time 1080, :type :invoke, :f :txn, :value [[:r 3 nil]]}
              {:process 0, :time 1090, :type :ok,     :f :txn, :value [[:r 3 [1 2 3]]]}])
          r (serializable/append-check h)]
      (is (:valid? r)))))

(deftest unrelated-appear-reorder
  (testing "Unrelated appends may be reordered"
    (let [h (h/history
             [{:process 0, :time 1000, :type :invoke, :f :txn, :value [[:r 1 nil] [:r 2 nil]]}
              {:process 1, :time 1010, :type :invoke, :f :txn, :value [[:append 1 1]]}
              {:process 1, :time 1020, :type :ok,     :f :txn, :value [[:append 1 1]]}
              {:process 1, :time 1030, :type :invoke, :f :txn, :value [[:append 2 1]]}
              {:process 1, :time 1040, :type :ok,     :f :txn, :value [[:append 2 1]]}
              {:process 0, :time 1050, :type :ok,     :f :txn, :value [[:r 1 nil] [:r 2 [1]]]}])
          r (serializable/append-check h)]
      (is (:valid? r) r))))
