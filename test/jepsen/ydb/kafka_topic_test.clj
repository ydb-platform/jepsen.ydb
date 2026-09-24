(ns jepsen.ydb.kafka-topic-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.kafka-topic :as kafka-topic])
  (:import (org.apache.kafka.common TopicPartition)))

(def test-map {:kafka-topic-name "topic"})

(deftest key-partition-mapping
  (testing "Keys map onto partitions of a single topic and back"
    (let [tp (kafka-topic/k->topic-partition test-map 7)]
      (is (= "topic" (.topic tp)))
      (is (= 7 (.partition tp)))
      (is (= 7 (kafka-topic/topic-partition->k test-map tp))))
    (is (= 3 (kafka-topic/topic-partition->k test-map (TopicPartition. "topic" 3))))))

(def txn-value
  [[:send 1 [0 10]]
   [:poll {1 [[5 50] [6 60]]
           2 [[3 30]]}]
   [:poll {1 [[7 70]]}]
   [:poll]])

(deftest polled-offsets
  (testing "First offsets polled per key"
    (is (= {1 5, 2 3} (kafka-topic/first-polled-offsets txn-value))))
  (testing "Highest offsets polled per key"
    (is (= {1 7, 2 3} (kafka-topic/highest-polled-offsets txn-value))))
  (testing "No polls"
    (is (= {} (kafka-topic/first-polled-offsets [[:send 1 [0 10]] [:poll]])))
    (is (= {} (kafka-topic/highest-polled-offsets [[:send 1 [0 10]] [:poll]])))))
