(ns jepsen.ydb
  (:gen-class)
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.cli :as cli]
            [jepsen.control :as c]
            [jepsen.control.net :as control-net]
            [jepsen.db :as db]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.net :as jepsen-net]
            [jepsen.net.proto :as net-proto]
            [jepsen.tests :as tests]
            [jepsen.ydb.cli.clean :refer [clean-valid-cmd]]
            [jepsen.ydb.append :as append]
            [jepsen.ydb.append-with-deletes :as append-with-deletes]))

(def dynamic-service "kikimr-multi@31003.service")
(def storage-service "kikimr.service")
(def jepsen-chain "JEPSEN_YDB")
(def jepsen-firewall-lock "/run/lock/jepsen-ydb-firewall.lock")

(defn ipv6?
  "Returns true when ip-str is an IPv6 address."
  [ip-str]
  (str/includes? ip-str ":"))

(defn firewall-command
  "Returns the firewall command for an IP address."
  [ip]
  (if (ipv6? ip) "ip6tables" "iptables"))

(defn add-drop-rule!
  "Ensures that the dedicated Jepsen chain exists and adds a DROP rule.

  Jepsen firewall modifications on a node are serialized with flock. The
  complete operation is retried because an external firewall manager may
  replace the ruleset between individual iptables commands."
  [cmd ip]
  (c/exec
    :bash :-c
    (str
      "flock -x " jepsen-firewall-lock " bash -c '"
      "last_error=\"\"; "
      "for attempt in 1 2 3 4 5; do "
      "  " cmd " -w -N " jepsen-chain " 2>/dev/null || true; "
      "  if ! " cmd " -w -C INPUT -j " jepsen-chain " 2>/dev/null; then "
      "    if ! last_error=$("
      cmd " -w -I INPUT 1 -j " jepsen-chain " 2>&1); then "
      "      echo \"Attempt ${attempt}: failed to install INPUT jump: "
      "${last_error}\" >&2; "
      "      sleep 0.1; "
      "      continue; "
      "    fi; "
      "  fi; "
      "  if last_error=$("
      cmd " -w -A " jepsen-chain " -s " ip " -j DROP 2>&1); then "
      "    exit 0; "
      "  fi; "
      "  echo \"Attempt ${attempt}: failed to add DROP rule: "
      "${last_error}\" >&2; "
      "  sleep 0.1; "
      "done; "
      "echo \"Unable to install Jepsen partition rule after 5 attempts: "
      "${last_error}\" >&2; "
      "exit 1'")))

(defn flush-jepsen-chain!
  "Flushes only rules owned by Jepsen.

  A missing chain is treated as an already-healed state. The operation uses the
  same lock as add-drop-rule! so local start/heal operations cannot race."
  [cmd]
  (c/exec
    :bash :-c
    (str
      "flock -x " jepsen-firewall-lock " bash -c '"
      "if " cmd " -w -L " jepsen-chain " -n >/dev/null 2>&1; then "
      "  " cmd " -w -F " jepsen-chain "; "
      "fi'")))

(def dual-stack-net
  "Drop-in replacement for jepsen.net/iptables which handles both IPv4 and
  IPv6 node addresses. Partition rules are stored in dedicated JEPSEN_YDB
  chains, so healing preserves infrastructure firewall rules."
  (reify net-proto/Net

    (drop! [_net test src dest]
      (c/on-nodes test [dest]
        (fn [_ _]
          (let [ip  (control-net/ip src)
                cmd (firewall-command ip)]
            (c/su
              (info "Adding partition rule"
                    {:destination dest
                     :source      src
                     :ip          ip
                     :command     cmd})
              (add-drop-rule! cmd ip))))))

    (heal! [_net test]
      (c/with-test-nodes test
        (c/su
          (info "Healing partition: flushing Jepsen chains only")
          (flush-jepsen-chain! "iptables")
          (flush-jepsen-chain! "ip6tables"))))

    (slow! [_net test]
      (c/with-test-nodes test
        (c/su
          (c/exec :tc :qdisc :add
                  :dev (jepsen-net/net-dev)
                  :root :netem
                  :delay :50ms :10ms
                  :distribution :normal))))

    (slow! [_net test {:keys [mean variance distribution]
                       :or   {mean 50
                              variance 10
                              distribution :normal}}]
      (c/with-test-nodes test
        (c/su
          (c/exec :tc :qdisc :add
                  :dev (jepsen-net/net-dev)
                  :root :netem
                  :delay (str mean "ms") (str variance "ms")
                  :distribution distribution))))

    (flaky! [_net test]
      (c/with-test-nodes test
        (c/su
          (c/exec :tc :qdisc :add
                  :dev (jepsen-net/net-dev)
                  :root :netem
                  :loss "20%" "75%"))))

    (fast! [_net test]
      (c/with-test-nodes test
        (try
          (c/su
            (c/exec :tc :qdisc :del
                    :dev (jepsen-net/net-dev)
                    :root))
          (catch RuntimeException e
            (when-not
              (re-find #"Error: Cannot delete qdisc with handle of zero\."
                       (.getMessage e))
              (throw e))))))

    (shape! [_net test nodes behavior]
      (jepsen-net/shape! jepsen-net/iptables test nodes behavior))

    net-proto/PartitionAll
    (drop-all! [_net test grudge]
      (c/on-nodes test (keys grudge)
        (fn [_ node]
          (c/su
            (doseq [src (get grudge node)]
              (let [ip  (control-net/ip src)
                    cmd (firewall-command ip)]
                (info "Adding partition rule"
                      {:destination node
                       :source      src
                       :ip          ip
                       :command     cmd})
                (add-drop-rule! cmd ip)))))))))

(defn sigkill-and-wait!
  "Sends SIGKILL to a systemd unit and blocks until it is inactive."
  [unit]
  (c/exec
    :bash :-c
    (str "systemctl kill -s SIGKILL " unit "; "
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

(defn pause-service!
  "Sends SIGSTOP to every process in a systemd unit. Errors are propagated."
  [unit]
  (info "Sending SIGSTOP" {:unit unit})
  (c/exec :systemctl :kill :--kill-who=all :-s :SIGSTOP unit))

(defn service-process-state
  "Returns MainPID and ps state for a systemd unit."
  [unit]
  (let [pid (str/trim
              (c/exec :systemctl :show unit :-p :MainPID :--value))]
    {:pid  pid
     :stat (when-not (= "0" pid)
             (str/trim
               (c/exec :ps :-o :stat= :-p pid)))}))

(defn resume-service!
  "Sends SIGCONT and verifies that the unit has a live, non-stopped MainPID."
  [unit]
  (info "Sending SIGCONT" {:unit unit})
  (c/exec :systemctl :kill :--kill-who=all :-s :SIGCONT unit)
  (let [{:keys [pid stat] :as state} (service-process-state unit)]
    (info "Service state after SIGCONT" (assoc state :unit unit))
    (when (or (= "0" pid)
              (str/blank? stat)
              (str/starts-with? stat "T"))
      (throw
        (ex-info "Service did not resume"
                 {:unit unit
                  :pid  pid
                  :stat stat}))))
  :resumed)

(defn safe-restart!
  "Restarts a systemd unit; logs a warning instead of crashing if it fails."
  [unit]
  (try
    (c/exec :systemctl :restart unit)
    (catch Exception e
      (warn "Failed to restart" unit ":" (.getMessage e)))))

(defn cleanup-node!
  "Removes residual partition rules and resumes YDB services on one node."
  [node]
  (info "YDB node cleanup started" {:node node})
  (c/su
    (flush-jepsen-chain! "iptables")
    (flush-jepsen-chain! "ip6tables")
    (resume-service! dynamic-service)
    (resume-service! storage-service))
  (info "YDB node cleanup completed" {:node node}))

(defn make-db []
  (reify db/DB
    (setup! [_ _test node]
      (info "YDB connection pre-check on node:" node)
      (cleanup-node! node))

    (teardown! [_ _test node]
      ;; This is intentionally repeated after final-generator. It protects the
      ;; next run if final recovery was incomplete or partially failed.
      (cleanup-node! node)
      (info "YDB testing finished on node:" node))

    db/Pause
    (pause! [_ _test node]
      (info "SIGSTOP dynamic+storage on" node)
      (c/su
        (pause-service! dynamic-service)
        (pause-service! storage-service))
      :paused)

    (resume! [_ _test node]
      (info "SIGCONT dynamic+storage on" node)
      (c/su
        (resume-service! dynamic-service)
        (resume-service! storage-service))
      :resumed)))

(def service-faults
  "All faults handled by the service nemesis."
  #{:kill-dynamic :kill-storage :restart-dynamic :restart-storage})

(def service-nemesis-fs
  "All :f values this nemesis can receive, including recovery operations."
  #{:kill-dynamic :kill-storage
    :restart-dynamic :restart-storage
    :start-dynamic :start-storage})

(defn service-nemesis
  "A nemesis that independently kills, restarts, or starts YDB service units."
  []
  (reify
    nemesis/Nemesis
    (setup! [this _test]
      this)

    (invoke! [_this test op]
      (let [target-nodes (or (:value op)
                             [(rand-nth (:nodes test))])]
        (c/on-nodes test target-nodes
          (fn [_ node]
            (c/su
              (case (:f op)
                :kill-dynamic
                (do
                  (info "SIGKILL dynamic on" node)
                  (sigkill-and-wait! dynamic-service))

                :kill-storage
                (do
                  (info "SIGKILL storage on" node)
                  (sigkill-and-wait! storage-service))

                :restart-dynamic
                (do
                  (info "Restarting dynamic on" node)
                  (safe-restart! dynamic-service))

                :restart-storage
                (do
                  (info "Restarting storage on" node)
                  (safe-restart! storage-service))

                :start-dynamic
                (do
                  (info "Starting dynamic on" node)
                  (safe-start! dynamic-service))

                :start-storage
                (do
                  (info "Starting storage on" node)
                  (safe-start! storage-service))))))
        (assoc op :value target-nodes)))

    (teardown! [_this _test])

    nemesis/Reflection
    (fs [_this]
      service-nemesis-fs)))

(defn- kill-cycle-gen
  "Infinite lazy sequence: kill-op → sleep → start-op → repeat."
  [kill-f start-f interval]
  (lazy-cat
    [{:type :info :f kill-f}
     (gen/sleep interval)
     {:type :info :f start-f}]
    (kill-cycle-gen kill-f start-f interval)))

(defn- restart-cycle-gen
  "Infinite lazy sequence: restart-op → sleep → repeat."
  [restart-f interval]
  (lazy-cat
    [{:type :info :f restart-f}
     (gen/sleep interval)]
    (restart-cycle-gen restart-f interval)))

(defn service-package
  [{:keys [faults interval nodes]
    :or   {interval 5}}]
  (let [active (filter service-faults faults)]
    (when (seq active)
      (let [per-fault-gens
            (for [f active]
              (case f
                :kill-dynamic
                (kill-cycle-gen
                  :kill-dynamic :start-dynamic interval)

                :kill-storage
                (kill-cycle-gen
                  :kill-storage :start-storage interval)

                :restart-dynamic
                (restart-cycle-gen
                  :restart-dynamic interval)

                :restart-storage
                (restart-cycle-gen
                  :restart-storage interval)))

            combined-gen (gen/mix per-fault-gens)]

        {:nemesis   (service-nemesis)
         :generator combined-gen
         :final-generator
         (gen/phases
           {:type :info :f :start-dynamic :value nodes}
           {:type :info :f :start-storage :value nodes})
         :perf
         #{{:name "kill-dynamic"
            :fs #{:kill-dynamic}
            :color "#E74C3C"}
           {:name "start-dynamic"
            :fs #{:start-dynamic}
            :color "#2ECC71"}
           {:name "kill-storage"
            :fs #{:kill-storage}
            :color "#C0392B"}
           {:name "start-storage"
            :fs #{:start-storage}
            :color "#27AE60"}
           {:name "restart-dynamic"
            :fs #{:restart-dynamic}
            :color "#F39C12"}
           {:name "restart-storage"
            :fs #{:restart-storage}
            :color "#E67E22"}}}))))

(defn ydb-workload [opts]
  (case (:workload-name opts)
    "append"
    (append/workload opts)

    "append-with-deletes"
    (append-with-deletes/workload opts)))

(defn ydb-unhandled-exceptions [opts]
  (let [wrapped (checker/unhandled-exceptions)]
    (if (:allow-exceptions opts)
      wrapped
      (reify checker/Checker
        (check [_this test history opts]
          (let [result (checker/check wrapped test history opts)]
            (if (and (:valid? result)
                     (seq (:exceptions result)))
              (merge result {:valid? false})
              result)))))))

(defn no-ssh?
  "Returns true when the CLI selected Jepsen's dummy remote transport."
  [opts]
  (or (false? (:ssh opts))
      (= :dummy (:ssh opts))
      (true? (get-in opts [:ssh :dummy]))))

(defn validate-opts
  "Validates that options are compatible with each other."
  [opts]
  (when (and (:with-opindex opts)
             (:model opts)
             (not= (:model opts) :ydb-serializable))
    (throw
      (IllegalArgumentException.
        "--with-opindex can be used with --model ydb-serializable only")))

  ;; Remote nemeses execute systemctl and firewall commands on test nodes.
  (when (and (seq (:nemesis opts))
             (no-ssh? opts))
    (throw
      (IllegalArgumentException.
        "--no-ssh cannot be used with remote YDB nemeses")))

  opts)

(defn ydb-test [opts]
  (validate-opts opts)
  (let [workload (ydb-workload opts)
        the-db   (make-db)

        nc-faults
        (filter #{:partition :clock :pause}
                (:nemesis opts))

        nc-pkgs
        (nc/nemesis-packages
          {:db        the-db
           :nodes     (:nodes opts)
           :faults    nc-faults
           :partition {:targets [:one]}
           :pause     {:targets [:one]}
           :interval  (:nemesis-interval opts)})

        svc-pkg
        (service-package
          {:faults   (:nemesis opts)
           :interval (:nemesis-interval opts)
           :nodes    (:nodes opts)})

        all-pkgs
        (remove nil?
                (concat
                  (filter (fn [package]
                            (some? (:generator package)))
                          nc-pkgs)
                  [svc-pkg]))

        composed
        (if (seq all-pkgs)
          (nc/compose-packages all-pkgs)
          {:nemesis         nemesis/noop
           :generator       nil
           :final-generator nil
           :perf            #{}})

        nem-gen   (:generator composed)
        final-gen (:final-generator composed)]

    (merge
      tests/noop-test
      opts
      {:name               "ydb"
       :db                 the-db
       :os                 ubuntu/os
       :net                dual-stack-net
       :concurrency-factor 1
       :client             (:client workload)
       :nemesis            (:nemesis composed)
       :checker
       (checker/compose
         {:perf
          (checker/perf {:nemeses (:perf composed)})

          :clock
          (checker/clock-plot)

          :stats
          (checker/stats)

          :exceptions
          (ydb-unhandled-exceptions opts)

          :workload
          (:checker workload)})

       :generator
       (gen/phases
         (->> (:generator workload)
              (gen/stagger (/ (:rate opts)))
              (gen/nemesis nem-gen)
              (gen/time-limit (:time-limit opts)))
         (gen/log "Recovering cluster state after test...")
         (when final-gen
           (gen/nemesis final-gen)))})))

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

(defn valid-probability? [value]
  (and (>= value 0.0)
       (<= value 1.0)))

(defn valid-read-replicas? [value]
  (>= value 0))

(def cli-opts
  [[nil "--db-name DBNAME"
    "YDB database name."
    :default "/local"]

   [nil "--db-port NUM"
    "YDB database port."
    :default 2135
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--db-table NAME"
    "YDB table name."
    :default "jepsen_test"]

   [nil "--workload-name NAME"
    "YDB workload name."
    :default "append"]

   [nil "--model MODEL"
    "Consistency model to check."
    :default :ydb-serializable
    :parse-fn keyword]

   [nil "--allow-exceptions"
    "Allow unhandled exceptions."
    :default false]

   [nil "--partition-size-mb NUM"
    "Table partition size in MBs."
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--initial-partition-count NUM"
    "Initial number of partitions."
    :default 30
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--initial-partition-keys NUM"
    "Initial number of keys per partition."
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--with-read-replicas NUM"
    "Per-az read replicas."
    :default 0
    :parse-fn parse-long
    :validate [valid-read-replicas? "Must be 0 or greater"]]

   [nil "--with-opindex"
    "Use additional opindex column."
    :default false]

   [nil "--with-changefeed"
    "Use updates changefeed."
    :default false]

   [nil "--batch-single-ops"
    "Execute single ops via batch query."
    :default false]

   [nil "--batch-ops-probability NUM"
    :default 0.0
    :parse-fn parse-double
    :validate [valid-probability? "Must be 0.0–1.0"]]

   [nil "--batch-commit-probability NUM"
    :default 1.0
    :parse-fn parse-double
    :validate [valid-probability? "Must be 0.0–1.0"]]

   [nil "--key-count NUM"
    "Keys in active rotation."
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--ballast-size NUM"
    "Ballast bytes added to values."
    :default 1000
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--max-txn-length NUM"
    "Max ops per transaction."
    :default 4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM"
    "Max writes to any key."
    :default 16
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   ["-r" "--rate HZ"
    "Approximate request rate in hz."
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be positive"]]

   [nil "--nemesis FAULTS"
    (str "Comma-separated nemesis faults. Valid values: "
         "partition, clock, pause, "
         "kill-dynamic, kill-storage, restart-dynamic, restart-storage, "
         "all, none. Omit entirely to run without any nemesis.")
    :default []
    :parse-fn parse-nemesis-spec
    :validate
    [(partial every? all-nemesis-faults)
     (str "Each fault must be one of: "
          (str/join ", " (map name all-nemesis-faults))
          ", all, none.")]]

   [nil "--nemesis-interval SECS"
    "Seconds between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be positive"]]

   [nil "--store-type TYPE"
    "Store type: 'row' or 'column'."
    :default "row"]])

(defn -main [& args]
  (cli/run!
    (merge
      (cli/single-test-cmd
        {:test-fn  ydb-test
         :opt-spec cli-opts})
      (cli/serve-cmd)
      (clean-valid-cmd))
    args))

