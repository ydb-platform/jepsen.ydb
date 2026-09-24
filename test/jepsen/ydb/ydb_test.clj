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

  (testing "Valid: kafka-topic with enough partitions"
    (let [opts {:workload-name "kafka-topic" :key-count 10 :kafka-partition-count 10}]
      (is (= opts (ydb/validate-opts opts)))))

  (testing "Invalid: kafka-topic with fewer partitions than keys"
    (is (thrown-with-msg? IllegalArgumentException
                          #"--kafka-partition-count must be >= --key-count"
                          (ydb/validate-opts {:workload-name "kafka-topic"
                                              :key-count 10
                                              :kafka-partition-count 9}))))

  (testing "Valid: other workloads ignore kafka-partition-count"
    (let [opts {:workload-name "append" :key-count 10 :kafka-partition-count 1}]
      (is (= opts (ydb/validate-opts opts))))))

