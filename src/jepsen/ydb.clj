(ns jepsen.ydb
  (:gen-class)
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.control :as c]
            [jepsen.control.net :as control-net]
            [jepsen.db :as db]
            [jepsen.os :as os]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.net :as jepsen-net]
            [jepsen.net.proto :as net-proto]
            [jepsen.tests :as tests]
            [jepsen.tests.kafka :as kafka]
            [jepsen.control.util :as cu]
            [jepsen.ydb.cli.clean :refer [clean-valid-cmd]]
            [jepsen.ydb.append :as append]
            [jepsen.ydb.append-with-deletes :as append-with-deletes]
            [jepsen.ydb.append-single-row :as append-single-row]
            [jepsen.ydb.kafka-topic :as kafka-topic]
            [jepsen.ydb.topic-table :as topic-table]))

(def dynamic-service "kikimr-multi@31003.service")
(def storage-service "kikimr.service")

(defn ipv6?
  "Returns true when ip-str is an IPv6 address (contains a colon)."
  [ip-str]
  (str/includes? ip-str ":"))

(def dual-stack-net
  "Drop-in replacement for jepsen.net/iptables that transparently handles both
  IPv4 (iptables) and IPv6 (ip6tables) node addresses."
  (reify net-proto/Net

    (drop! [net test src dest]
      (c/on-nodes test [dest]
        (fn [_ _]
          (let [ip  (control-net/ip src)
                cmd (if (ipv6? ip) :ip6tables :iptables)]
            (c/su (c/exec cmd :-A :INPUT :-s ip :-j :DROP :-w))))))

    (heal! [net test]
      (c/with-test-nodes test
        (c/su
          (try
            (c/exec :iptables :-F :-w)
            (catch Exception e
              (warn "iptables -F -w failed during heal!:" (.getMessage e))))
          (try
            (c/exec :ip6tables :-F :-w)
            (catch Exception e
              (warn "ip6tables -F -w failed during heal!:" (.getMessage e)))))))

    (slow! [net test]
      (c/with-test-nodes test
        (c/su (c/exec :tc :qdisc :add :dev (jepsen-net/net-dev)
                      :root :netem :delay :50ms :10ms
                      :distribution :normal))))

    (slow! [net test {:keys [mean variance distribution]
                      :or   {mean 50 variance 10 distribution :normal}}]
      (c/with-test-nodes test
        (c/su (c/exec :tc :qdisc :add :dev (jepsen-net/net-dev)
                      :root :netem :delay
                      (str mean "ms") (str variance "ms")
                      :distribution distribution))))

    (flaky! [net test]
      (c/with-test-nodes test
        (c/su (c/exec :tc :qdisc :add :dev (jepsen-net/net-dev)
                      :root :netem :loss "20%" "75%"))))

    (fast! [net test]
      (c/with-test-nodes test
        (try
          (c/su (c/exec :tc :qdisc :del :dev (jepsen-net/net-dev) :root))
          (catch RuntimeException e
            (when-not (re-find #"Error: Cannot delete qdisc with handle of zero\."
                               (.getMessage e))
              (throw e))))))

    (shape! [net test nodes behavior]
      (jepsen-net/shape! jepsen-net/iptables test nodes behavior))

    net-proto/PartitionAll
    (drop-all! [net test grudge]
      (c/on-nodes test (keys grudge)
        (fn [_ node]
          (let [srcs  (get grudge node)
                ips   (map control-net/ip srcs)
                ipv4s (remove ipv6? ips)
                ipv6s (filter ipv6? ips)]
            (c/su
              (when (seq ipv4s)
                (c/exec :iptables :-A :INPUT :-s (str/join "," ipv4s) :-j :DROP :-w))
              (when (seq ipv6s)
                (c/exec :ip6tables :-A :INPUT :-s (str/join "," ipv6s) :-j :DROP :-w)))))))))

(defn sigkill-and-wait!
  "Sends SIGKILL to a systemd unit and blocks until it is inactive."
  [unit]
  (c/exec :bash :-c (str "systemctl kill -s SIGKILL " unit "; "
                          "while systemctl is-active --quiet " unit "; do "
                          "  sleep 0.2; "
                          "done")))

(defn safe-start!
  "Starts a systemd unit; logs a warning instead of crashing if it fails."
  [unit]
  (try
    (c/exec :systemctl :start unit)
    (catch Exception e
      (warn "Failed to start" unit ":" (.getMessage e)))))

(defn safe-pause!
  "Sends SIGSTOP to a systemd unit; logs a warning if the unit is not found."
  [unit]
  (try
    (c/exec :systemctl :kill :-s :SIGSTOP unit)
    (catch Exception e
      (warn "Failed to pause" unit ":" (.getMessage e)))))

(defn safe-resume!
  "Sends SIGCONT to a systemd unit; logs a warning if the unit is not found."
  [unit]
  (try
    (c/exec :systemctl :kill :-s :SIGCONT unit)
    (catch Exception e
      (warn "Failed to resume" unit ":" (.getMessage e)))))

(defn safe-restart!
  "Restarts a systemd unit; logs a warning instead of crashing if it fails."
  [unit]
  (try
    (c/exec :systemctl :restart unit)
    (catch Exception e
      (warn "Failed to restart" unit ":" (.getMessage e)))))

(defn make-db []
  (reify db/DB
    (setup! [_ _test node]
      (info "YDB connection pre-check on node:" node))
    (teardown! [_ _test node]
      (info "YDB testing finished on node:" node))

    db/Pause
    (pause! [_ _test node]
      (info "SIGSTOP dynamic+storage on" node)
      (c/su
        (safe-pause! dynamic-service)
        (safe-pause! storage-service))
      :paused)

    (resume! [_ _test node]
      (info "SIGCONT dynamic+storage on" node)
      (c/su
        (safe-resume! dynamic-service)
        (safe-resume! storage-service))
      :resumed)))

(def service-faults
  "All faults handled by the service nemesis."
  #{:kill-dynamic :kill-storage :restart-dynamic :restart-storage})

(def service-nemesis-fs
  "All operations handled by the service nemesis, including recovery operations."
  #{:kill-dynamic :kill-storage
    :restart-dynamic :restart-storage
    :start-dynamic :start-storage})

(defn service-nemesis
  "A nemesis that independently kills, restarts, or starts YDB service units."
  []
  (reify
    nemesis/Nemesis
    (setup! [this _test] this)

    (invoke! [_this test op]
      (let [target-nodes (or (:value op) [(rand-nth (:nodes test))])]
        (c/on-nodes test target-nodes
          (fn [_ node]
            (c/su
              (case (:f op)
                :kill-dynamic
                (do (info "SIGKILL dynamic on" node)
                    (sigkill-and-wait! dynamic-service))

                :kill-storage
                (do (info "SIGKILL storage on" node)
                    (sigkill-and-wait! storage-service))

                :restart-dynamic
                (do (info "Restarting dynamic on" node)
                    (safe-restart! dynamic-service))

                :restart-storage
                (do (info "Restarting storage on" node)
                    (safe-restart! storage-service))

                :start-dynamic
                (do (info "Starting dynamic on" node)
                    (safe-start! dynamic-service))

                :start-storage
                (do (info "Starting storage on" node)
                    (safe-start! storage-service))))))
        (assoc op :value target-nodes)))

    (teardown! [_this _test])

    nemesis/Reflection
    (fs [_this] service-nemesis-fs)))

(defn service-package
  "Builds the service nemesis package. Its generator is used only while composing
  the package router; ydb-test replaces the composed generator with the strict
  global generator below."
  [{:keys [faults interval nodes] :or {interval 5}}]
  (let [active (filter service-faults faults)]
    (when (seq active)
      {:nemesis  (service-nemesis)
       :generator
       (gen/mix
         (for [fault active]
           (repeat {:type :info :f fault})))
       :final-generator
       (gen/phases
         {:type :info :f :start-dynamic :value nodes}
         {:type :info :f :start-storage :value nodes})
       :perf
       #{{:name "kill-dynamic"    :fs #{:kill-dynamic}    :color "#E74C3C"}
         {:name "start-dynamic"   :fs #{:start-dynamic}   :color "#2ECC71"}
         {:name "kill-storage"    :fs #{:kill-storage}    :color "#C0392B"}
         {:name "start-storage"   :fs #{:start-storage}   :color "#27AE60"}
         {:name "restart-dynamic" :fs #{:restart-dynamic} :color "#F39C12"}
         {:name "restart-storage" :fs #{:restart-storage} :color "#E67E22"}}})))

(defn- fault-operations
  "Returns [attack recovery] for one logical fault. Recovery is nil for restart
  faults because systemctl restart is a complete, instantaneous disruption.
  Service recovery targets the same node selected for the attack."
  [fault nodes]
  (case fault
    :partition
    [{:type :info :f :start-partition :value :one}
     {:type :info :f :stop-partition}]

    :clock
    [{:type :info :f :bump-clock}
     {:type :info :f :reset-clock}]

    :pause
    [{:type :info :f :pause :value :one}
     {:type :info :f :resume}]

    :kill-dynamic
    (let [target [(rand-nth nodes)]]
      [{:type :info :f :kill-dynamic :value target}
       {:type :info :f :start-dynamic :value target}])

    :kill-storage
    (let [target [(rand-nth nodes)]]
      [{:type :info :f :kill-storage :value target}
       {:type :info :f :start-storage :value target}])

    :restart-dynamic
    (let [target [(rand-nth nodes)]]
      [{:type :info :f :restart-dynamic :value target}
       nil])

    :restart-storage
    (let [target [(rand-nth nodes)]]
      [{:type :info :f :restart-storage :value target}
       nil])))

(defn- strict-nemesis-cycle
  "Infinite global sequence:

  random attack -> attack-duration -> matching recovery -> rest-duration.

  Only one attack can be active at a time. A new random fault is selected only
  after the previous fault has been recovered and the rest period has elapsed."
  [faults nodes attack-duration rest-duration]
  (lazy-seq
    (let [fault             (rand-nth (vec faults))
          [attack recovery] (fault-operations fault nodes)
          cycle             (cond-> [attack
                                     (gen/sleep attack-duration)]
                              recovery (conj recovery)
                              true     (conj (gen/sleep rest-duration)))]
      (concat cycle
              (strict-nemesis-cycle faults
                                    nodes
                                    attack-duration
                                    rest-duration)))))

(defn ydb-workload [opts]
  (case (:workload-name opts)
    "append"              (append/workload opts)
    "append-with-deletes" (append-with-deletes/workload opts)
    "append-single-row"   (append-single-row/workload opts)
    "kafka-topic"         (kafka-topic/workload opts)
    "topic-table"         (topic-table/workload opts)))

(defn ydb-unhandled-exceptions [opts]
  (let [wrapped (checker/unhandled-exceptions)]
    (if (:allow-exceptions opts)
      wrapped
      (reify checker/Checker
        (check [_this test history opts]
          (let [result (checker/check wrapped test history opts)]
            (if (and (:valid? result) (seq (:exceptions result)))
              (merge result {:valid? false})
              result)))))))

(defn validate-opts
  "Validates that options are compatible with each other."
  [opts]
  (when (and (:with-opindex opts)
             (:model opts)
             (not= (:model opts) :ydb-serializable))
    (throw (IllegalArgumentException.
             "--with-opindex can be used with --model ydb-serializable only")))
  (when (and (= (:workload-name opts) "kafka-topic")
             (< (:kafka-partition-count opts) (:key-count opts)))
    (throw (IllegalArgumentException.
             "--kafka-partition-count must be >= --key-count for --workload-name kafka-topic")))
  (when (and (= (:workload-name opts) "topic-table")
             (not= (:model opts) :ydb-serializable))
    (throw (IllegalArgumentException.
             "--workload-name topic-table requires --model ydb-serializable (YDB topic transactions require SERIALIZABLE_RW)")))
  opts)

(defn ydb-test [opts]
  (validate-opts opts)
  (let [workload        (ydb-workload opts)
        the-db          (make-db)
        faults          (vec (:nemesis opts))
        attack-duration (:nemesis-attack-duration opts)
        rest-duration   (:nemesis-rest-duration opts)

        nc-faults (filter #{:partition :clock :pause} faults)
        nc-pkgs   (nc/nemesis-packages
                    {:db        the-db
                     :nodes     (:nodes opts)
                     :faults    nc-faults
                     :partition {:targets [:one]}
                     :pause     {:targets [:one]}
                     :clock     {:drift 60}
                     :interval  attack-duration})

        svc-pkg (service-package
                  {:faults   faults
                   :interval attack-duration
                   :nodes    (:nodes opts)})

        all-pkgs (remove nil?
                   (concat
                     (filter (fn [package]
                               (some? (:generator package)))
                             nc-pkgs)
                     [svc-pkg]))

        composed (if (seq all-pkgs)
                   (nc/compose-packages all-pkgs)
                   {:nemesis         nemesis/noop
                    :generator       nil
                    :final-generator nil
                    :perf            #{}})

        nem-gen (when (seq faults)
                  (strict-nemesis-cycle faults
                                        (:nodes opts)
                                        attack-duration
                                        rest-duration))
        final-gen (:final-generator composed)]

    (merge tests/noop-test
           opts
           (select-keys workload [:sub-via :txn? :crash-clients? :crash-client-interval])
           {:name               "ydb"
            :db                 the-db
            :os                 ubuntu/os
            :net                dual-stack-net
            :concurrency-factor 1
            :ssh                {:dummy false :strict-host-key-checking false}
            :client             (:client workload)
            :nemesis            (:nemesis composed)
            :checker            (checker/compose
                                  {:perf       (checker/perf
                                                 {:nemeses (:perf composed)})
                                   :clock      (checker/clock-plot)
                                   :stats      (kafka/stats-checker (checker/stats))
                                   :exceptions (ydb-unhandled-exceptions opts)
                                   :workload   (:checker workload)})

            :generator
            (gen/phases
              (->> (:generator workload)
                   (gen/stagger (/ (:rate opts)))
                   (gen/nemesis nem-gen)
                   (gen/time-limit (:time-limit opts)))
              (gen/log "Recovering cluster state after test...")
              (when final-gen
                (gen/nemesis final-gen))
              (when-let [workload-final-gen (:final-generator workload)]
                (gen/phases
                  (gen/log "Running workload final generator...")
                  (gen/sleep 10)
                  (gen/time-limit (:kafka-final-time-limit opts)
                                  (gen/clients workload-final-gen)))))})))

(def all-nemesis-faults
  "Full set of accepted nemesis fault keywords."
  #{:pause :partition :clock
    :kill-dynamic :kill-storage
    :restart-dynamic :restart-storage})

(def special-nemeses
  "Named shorthand groups."
  {:none []
   :all  (vec all-nemesis-faults)})

(defn parse-nemesis-spec
  "Parses a comma-separated nemesis string into a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (map (comp keyword str/trim))
       (mapcat #(get special-nemeses % [%]))))

(defn valid-probability? [v]
  (and (>= v 0.0) (<= v 1.0)))

(defn valid-read-replicas? [v]
  (>= v 0))

(defn random-kafka-password
  "A random password for the kafka-topic workload's self-provisioned YDB
   user, generated once per test run rather than checked into source. Hex
   only, since YDB's default password policy only allows letters, digits,
   and a specific set of special characters, and this sidesteps that set
   entirely."
  []
  (str/replace (str (random-uuid)) "-" ""))

(def cli-opts
  [[nil "--db-name DBNAME"               "YDB database name."
    :default "/local"]
   [nil "--db-port NUM"                  "YDB database port."
    :default 2135 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--db-table NAME"                "YDB table name."
    :default "jepsen_test"]
   [nil "--workload-name NAME"           "YDB workload name."
    :default "append"]
   [nil "--model MODEL"                  "Consistency model to check."
    :default :ydb-serializable :parse-fn keyword]
   [nil "--allow-exceptions"             "Allow unhandled exceptions."
    :default false]
   [nil "--partition-size-mb NUM"        "Table partition size in MBs."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--initial-partition-count NUM"  "Initial number of partitions."
    :default 30 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--initial-partition-keys NUM"   "Initial number of keys per partition."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--with-read-replicas NUM"       "Per-az read replicas."
    :default 0 :parse-fn parse-long :validate [valid-read-replicas? "Must be 0 or greater"]]
   [nil "--with-opindex"                 "Use additional opindex column."
    :default false]
   [nil "--with-changefeed"              "Use updates changefeed."
    :default false]
   [nil "--batch-single-ops"             "Execute single ops via batch query."
    :default false]
   [nil "--batch-ops-probability NUM"
    :default 0.0 :parse-fn parse-double :validate [valid-probability? "Must be 0.0–1.0"]]
   [nil "--batch-commit-probability NUM"
    :default 1.0 :parse-fn parse-double :validate [valid-probability? "Must be 0.0–1.0"]]
   [nil "--key-count NUM"                "Keys in active rotation."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--ballast-size NUM"             "Ballast bytes added to values."
    :default 1000 :parse-fn parse-long :validate [pos? "Must be positive"]]
   [nil "--max-txn-length NUM"           "Max ops per transaction."
    :default 4 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--max-writes-per-key NUM"       "Max writes to any key."
    :default 16 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   ["-r" "--rate HZ"                     "Approximate request rate in hz."
    :default 100 :parse-fn read-string :validate [pos? "Must be positive"]]

   [nil "--nemesis FAULTS"
    (str "Comma-separated nemesis faults. Valid values: "
         "partition, clock, pause, "
         "kill-dynamic, kill-storage, restart-dynamic, restart-storage, "
         "all, none. Omit entirely to run without any nemesis.")
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? all-nemesis-faults)
               (str "Each fault must be one of: "
                    (str/join ", " (map name all-nemesis-faults))
                    ", all, none.")]]

   [nil "--nemesis-attack-duration SECS"
    "Seconds for which the selected nemesis attack remains active."
    :default 5 :parse-fn read-string :validate [pos? "Must be positive"]]
   [nil "--nemesis-rest-duration SECS"
    "Seconds to wait after recovery before selecting the next attack."
    :default 5 :parse-fn read-string :validate [pos? "Must be positive"]]
   [nil "--store-type TYPE"              "Store type: 'row' or 'column'."
    :default "row"]

   [nil "--topic-name NAME"              "YDB topic name (for --workload-name topic-table)."
    :default "jepsen_test_topic"]
   [nil "--topic-partition-count NUM"
    "Fixed number of partitions for --workload-name topic-table. Keys are
     hashed onto these partitions, since the elle generator's key space is
     unbounded over the life of a test."
    :default 30 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--table-key-count NUM"          "Table keys in active rotation, for --workload-name topic-table."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--topic-key-count NUM"          "Topic keys in active rotation, for --workload-name topic-table."
    :default 10 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]

   [nil "--kafka-port NUM"               "YDB Kafka API port."
    :default 9092 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--kafka-topic-name NAME"        "Topic used by the kafka-topic workload."
    :default "jepsen_kafka_topic"]
   [nil "--kafka-partition-count NUM"    "Topic partitions, must be >= --key-count."
    :default 64 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--[no-]kafka-txn"               "Use Kafka producer transactions."
    :id :txn? :default true]
   [nil "--kafka-isolation-level LEVEL"  "Consumer isolation level."
    :default "read_committed"
    :validate [#{"read_committed" "read_uncommitted"}
               "Must be read_committed or read_uncommitted"]]
   [nil "--kafka-transaction-timeout-ms NUM"
    "Kafka transaction timeout in ms. Must stay above the producer's
     delivery.timeout.ms (15000, see kafka-client/producer-config) -- otherwise
     the transaction coordinator can time out and abort a transaction while a
     send is still legitimately retrying, and comfortably below the server's
     kafka_proxy_config.transaction_timeout_ms (default 300000)."
    :default 30000 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]
   [nil "--[no-]kafka-sasl"              "Authenticate via SASL_PLAINTEXT. Needed on YDB to select the target database over the Kafka protocol; see README."
    :id :kafka-sasl? :default true]
   [nil "--kafka-sasl-mechanism NAME"    "SASL mechanism: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512."
    :default "PLAIN"
    :validate [#{"PLAIN" "SCRAM-SHA-256" "SCRAM-SHA-512"}
               "Must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512"]]
   [nil "--kafka-username NAME"          "SASL username. Created on the cluster during setup if missing. With PLAIN, --db-name is appended automatically unless already present."
    :default "jepsen"
    :validate [#(not (str/includes? % "'")) "Must not contain a single quote (used unescaped in YQL)"]]
   [nil "--kafka-password PASS"          "SASL password. Defaults to a fresh random value generated for this run."
    :default (random-kafka-password)
    :validate [#(not (str/includes? % "'")) "Must not contain a single quote (used unescaped in YQL)"]]
   [nil "--kafka-crash-clients"          "Periodically crash and reopen Kafka clients."
    :id :crash-clients? :default false]
   [nil "--kafka-crash-client-interval SECS" "Seconds between client crashes."
    :id :crash-client-interval :default 30 :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]
   [nil "--kafka-final-time-limit SECS"
    "Seconds allowed for the workload final generator (jepsen.tests.kafka's
     catch-up read of every key, seeking to the beginning of each partition).
     jepsen.tests.kafka's own generator has no meaningful time budget of its
     own here -- it keeps polling until every key is caught up -- so this is
     the only real cutoff. Too low a value for the configured --key-count/
     --max-writes-per-key/--concurrency cuts the catch-up read off before it
     finishes, which the checker can't distinguish from genuine data loss:
     it'll report spurious :unseen failures on an otherwise-correct run.
     Scale this up for larger workloads."
    :default 300 :parse-fn parse-long :validate [pos? "Must be a positive integer"]]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn ydb-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd)
                   (clean-valid-cmd))
            args))

