(ns jepsen.ydb.ydb-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb :as ydb]))

(deftest validate-opts-test
  (testing "Valid: ydb-serializable without opindex"
    (is (= {:model :ydb-serializable :with-opindex false}
           (ydb/validate-opts {:model :ydb-serializable :with-opindex false}))))

  (testing "Valid: ydb-serializable with opindex"
    (is (= {:model :ydb-serializable :with-opindex true}
           (ydb/validate-opts {:model :ydb-serializable :with-opindex true}))))

  (testing "Valid: snapshot-isolation without opindex"
    (is (= {:model :snapshot-isolation :with-opindex false}
           (ydb/validate-opts {:model :snapshot-isolation :with-opindex false}))))

  (testing "Valid: snapshot-isolation without opindex key"
    (is (= {:model :snapshot-isolation}
           (ydb/validate-opts {:model :snapshot-isolation}))))

  (testing "Invalid: snapshot-isolation with opindex"
    (is (thrown? IllegalArgumentException
                (ydb/validate-opts {:model :snapshot-isolation
                                    :with-opindex true}))))

  (testing "Valid: read-committed without opindex"
    (is (= {:model :read-committed :with-opindex false}
           (ydb/validate-opts {:model :read-committed :with-opindex false}))))

  (testing "Valid: read-committed without opindex key"
    (is (= {:model :read-committed}
           (ydb/validate-opts {:model :read-committed}))))

  (testing "Invalid: read-committed with opindex"
    (is (thrown? IllegalArgumentException
                (ydb/validate-opts {:model :read-committed
                                    :with-opindex true}))))

  (testing "Valid: empty opts"
    (is (= {} (ydb/validate-opts {}))))

  (testing "Valid: no model specified"
    (is (= {:with-opindex true}
           (ydb/validate-opts {:with-opindex true}))))

  (testing "Invalid: check error message"
    (is (thrown-with-msg? IllegalArgumentException
                          #"with-opindex can be used with --model ydb-serializable only"
                          (ydb/validate-opts {:model :snapshot-isolation
                                              :with-opindex true}))))

  (testing "Valid: append-single-row-to-topic with ydb-serializable"
    (is (= {:model :ydb-serializable :workload-name "append-single-row-to-topic"}
           (ydb/validate-opts {:model :ydb-serializable :workload-name "append-single-row-to-topic"}))))

  (testing "Invalid: append-single-row-to-topic with snapshot-isolation"
    (is (thrown-with-msg? IllegalArgumentException
                          #"append-single-row-to-topic requires --model ydb-serializable"
                          (ydb/validate-opts {:model :snapshot-isolation
                                              :workload-name "append-single-row-to-topic"}))))

  (testing "Invalid: append-single-row-to-topic without a model"
    (is (thrown-with-msg? IllegalArgumentException
                          #"append-single-row-to-topic requires --model ydb-serializable"
                          (ydb/validate-opts {:workload-name "append-single-row-to-topic"})))))

