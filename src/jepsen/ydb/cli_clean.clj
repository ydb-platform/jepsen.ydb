(ns jepsen.ydb.cli-clean
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.cli :as cli]
            [jepsen.store :as store]
            [clj-time.coerce :as time.coerce]
            [clj-time.format :as time.format]))

(def basic-date-time (time.format/formatters :basic-date-time))

(defn parse-time
  "Parses a time from a string"
  [t]
  (-> (time.format/parse-local basic-date-time t)
      time.coerce/to-date-time))

(defn sorted-tests
  []
  (->> (for [[name runs] (store/tests)
             [time test] runs]
         {:name name,
          :time time
          :start-time (parse-time time)})
       (sort-by :start-time)
       reverse))

(defn clean-valid
  [{:keys [options]}]
  (let [tests (sorted-tests)
        tests (drop (:keep options) tests)]
    (doall
     (for [t tests]
       (do
         (let [valid (:valid? (store/load-results (:name t) (:time t)) :incomplete)]
           (if (or (= valid true)
                   (= valid :incomplete))
             (do
               (info "Removing valid test" (:name t) (:time t))
               (store/delete! (:name t) (:time t)))
             (info "Skipping test" (:name t) (:time t)))))))))

(defn clean-valid-cmd
  "A clean-valid command"
  []
  {"clean-valid" {:opt-spec [cli/help-opt
                             ["-k", "--keep NUMBER" "Number of most recent results to keep"
                              :default 4
                              :parse-fn #(Long/parseLong %)
                              :validate [pos? "Must be positive"]]]
                  :run clean-valid}})
