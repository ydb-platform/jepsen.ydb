(ns jepsen.ydb.topic-test
  (:require [clojure.test :refer [deftest testing is]]
            [jepsen.ydb.topic :as topic]))

(deftest hash-key-onto-test
  (testing "Keys within the id count map onto the corresponding id"
    (is (= 0 (topic/hash-key-onto [0 1 2 3 4] 0)))
    (is (= 4 (topic/hash-key-onto [0 1 2 3 4] 4))))
  (testing "Keys beyond the id count wrap around"
    ; elle.txn/fresh-key produces an unbounded, ever-increasing key space
    ; (see elle.txn/wr-txns), so keys well beyond the partition count must
    ; still map onto a valid partition.
    (is (= 0 (topic/hash-key-onto [0 1 2 3 4] 5)))
    (is (= 3 (topic/hash-key-onto [0 1 2 3 4] 100003))))
  (testing "Partition ids need not be a dense [0, N) range"
    ; observed in practice: YDB doesn't guarantee partition ids are a dense
    ; 0-based range, so hashing must work against whatever ids the server
    ; actually reports.
    (is (= 55 (topic/hash-key-onto [3 7 12 55] 3)))
    (is (= 3 (topic/hash-key-onto [3 7 12 55] 4)))))

(deftest message-round-trip-test
  (testing "Encoding and decoding a message preserves key and value"
    (is (= [3 42] (topic/decode-message (topic/encode-message 3 42)))))
  (testing "Large keys and values round-trip"
    (is (= [123456789 987654321] (topic/decode-message (topic/encode-message 123456789 987654321))))))
