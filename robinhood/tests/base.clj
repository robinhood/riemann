(ns robinhood.tests.base
  (:require (riemann [test :refer [tap io inject! deftest]])))

(riemann.test/tests

 (require '[riemann.test :refer [tap io inject! deftest]]
          '[riemann.config :refer [streams]]
          '[clojure.test.check :as tc]
          '[clojure.test.check.clojure-test :refer [defspec]]
          '[clojure.test.check.generators :as gen]
          '[clojure.test.check.properties :as prop]
          '[robinhood.consumers.processor :refer [process-single-metric-rule]]
          '[robinhood.rules.common :refer [check-event-tags]]
          '[robinhood.rules.base :refer [when-equal when-lower when-higher
                                         when-not-equal when-in-range when-out-of-range]])

 (defmethod clojure.test/report :begin-test-var [m]
   (println (-> m :var meta :name)))

 (let [base-test-bucket (tap :base-test-bucket riemann.streams/bit-bucket)]

   (defn update-conditions [stream-name conditions]
     (assoc conditions
       :thread-through [stream-name]
       :alert base-test-bucket))

   (let [trigger-condition {:service "cpu.utilization"
                            :metric-name "cpu.utilization"
                            :metric 30
                            :tags ["bastion"]
                            :type "usage"}
         event (assoc trigger-condition :time 1 :host "foo.bar.com")]

     (deftest test-when-lower
       (let [test-event (assoc event :metric 20)
             conditions (update-conditions when-lower trigger-condition)]
         (is (= (:base-test-bucket
                 (inject! [(process-single-metric-rule conditions base-test-bucket)]
                          [test-event]))
                [(assoc test-event :service-status :problem)])))))

   (defn count-passing-streams [condition event stream-names desired-output]
     (let [test-streams (map #(process-single-metric-rule (update-conditions %1 condition)
                                            base-test-bucket)
                             stream-names)]
       (count (filter #(= (:base-test-bucket (inject! [%] [event]))
                          desired-output)
                      test-streams))))

   ;; Every event and trigger condition we generate should satisfy the following conditions.
   ;;  1. Trigger a rule when metric is either <, > or = condition value
   ;;  2. Exactly one rule should be triggered
   ;;  3. The rules should not trigger if there are additional conditions that
   ;;     need to be met, but aren't satisfied
   (defspec eql-or-not-tests 100
     (prop/for-all [metric-value gen/nat
                    condition-value gen/nat
                    service (gen/not-empty gen/string-alphanumeric)
                    seed-hashes (gen/let [tags (gen/not-empty
                                                (gen/vector-distinct
                                                 (gen/not-empty
                                                  gen/string-alphanumeric)))
                                          kvs (gen/not-empty
                                               (gen/map
                                                gen/keyword
                                                (gen/not-empty
                                                 gen/string-alphanumeric)))]
                                  {:event (conj {:tags (random-sample 0.5 tags)}
                                                (conj {} (random-sample 0.5 kvs)))
                                   :condition (conj {:roles (random-sample 0.2 tags)}
                                                    (conj {} (random-sample 0.2 kvs)))})]

                   (let [test-event (conj {:service service :metric metric-value
                                           :metric-name service :time 1}
                                          (:event seed-hashes))
                         trigger-condition (conj {:service service :metric condition-value
                                                  :metric-name service}
                                                 (:condition seed-hashes))
                         stream-names [when-equal
                                       when-not-equal]]

                     (or (and (= 1 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :problem)]))
                              (check-event-tags trigger-condition test-event))
                         (and (= 0 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :ok)]))
                              (not (check-event-tags trigger-condition test-event)))))))


   ;; Every event and trigger condition we generate should satisfy the following conditions.
   ;;  1. Trigger a rule when metric is either <, > or = condition value
   ;;  2. Exactly one rule should be triggered
   ;;  3. The rules should not trigger if there are additional conditions that
   ;;     need to be met, but aren't satisfied
   (defspec min-max-eql-tests 100
     (prop/for-all [metric-value gen/nat
                    condition-value gen/nat
                    service (gen/not-empty gen/string-alphanumeric)
                    seed-hashes (gen/let [tags (gen/not-empty
                                                (gen/vector-distinct
                                                 (gen/not-empty
                                                  gen/string-alphanumeric)))
                                          kvs (gen/not-empty
                                               (gen/map
                                                gen/keyword
                                                (gen/not-empty
                                                 gen/string-alphanumeric)))]
                                  {:event (conj {:tags (random-sample 0.5 tags)}
                                                (conj {} (random-sample 0.5 kvs)))
                                   :condition (conj {:roles (random-sample 0.2 tags)}
                                                    (conj {} (random-sample 0.2 kvs)))})]

                   (let [test-event (conj {:service service :metric metric-value
                                           :metric-name service :time 1}
                                          (:event seed-hashes))
                         trigger-condition (conj {:service service :metric condition-value
                                                  :metric-name service}
                                                 (:condition seed-hashes))
                         stream-names [when-higher
                                       when-lower
                                       when-equal]]

                     (or (and (= 1 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :problem)]))
                              (= 2 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :ok)]))
                              (check-event-tags trigger-condition test-event))
                         (and (= 0 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :problem)]))
                              (= 0 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :ok)]))
                              (not (check-event-tags trigger-condition test-event)))))))



   (defspec range-tests 100
     (prop/for-all [metric-value gen/nat
                    range-min gen/nat
                    range gen/nat
                    service (gen/not-empty gen/string-alphanumeric)
                    seed-hashes (gen/let [tags (gen/not-empty
                                                (gen/vector-distinct
                                                 (gen/not-empty
                                                  gen/string-alphanumeric)))
                                          kvs (gen/not-empty
                                               (gen/map
                                                gen/keyword
                                                (gen/not-empty
                                                 gen/string-alphanumeric)))]
                                  {:event (conj {:tags (random-sample 0.5 tags)}
                                                (conj {} (random-sample 0.5 kvs)))
                                   :condition (conj {:roles (random-sample 0.2 tags)}
                                                    (conj {} (random-sample 0.2 kvs)))})]

                   (let [test-event (conj {:service service
                                           :metric-name service
                                           :metric metric-value :time 1}
                                          (:event seed-hashes))
                         trigger-condition (conj {:metric-name service :metric [range-min (+ range-min range)]}
                                                 (:condition seed-hashes))
                         stream-names [when-in-range
                                       when-out-of-range]]

                     (or (and (= 1 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :problem)]))
                              (= 1 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :ok)]))
                              (check-event-tags trigger-condition test-event))
                         (and (= 0 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :problem)]))
                              (= 0 (count-passing-streams trigger-condition test-event
                                                          stream-names
                                                          [(assoc test-event :service-status :ok)]))
                              (not (check-event-tags trigger-condition test-event)))))))))
