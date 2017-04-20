(ns robinhood.tests.folds
  (:require (riemann [test :refer [tap io inject! deftest]])))

(riemann.test/tests

 (require '[riemann.test :refer [tap io inject! deftest]]
          '[clojure.test.check :as tc]
          '[clojure.test.check.clojure-test :refer [defspec]]
          '[clojure.test.check.generators :as gen]
          '[clojure.test.check.properties :as prop]
          '[robinhood.consumers.processor :refer [process-single-metric-rule]]
          '[robinhood.rules.common :refer [check-event-tags]]
          '[robinhood.rules.folds :refer [mean]]
          '[robinhood.rules.base :refer [when-equal when-lower when-higher
                                        when-in-range when-out-of-range]])

 (defmethod clojure.test/report :begin-test-var [m]
   (println (-> m :var meta :name)))

 (let [folds-test-bucket (tap :folds-test-bucket riemann.streams/bit-bucket)]

   (let [trigger-condition {:service "cpu.utilization"
                            :metric 30
                            :tags ["bastion"]
                            :type "usage"}
         event1 (assoc trigger-condition :time 1 :host "foo.example.com")
         event2 (assoc trigger-condition :time 6 :host "foo.example.com")
         event3 (assoc trigger-condition :time 13 :host "foo.example.com")]

     (deftest test-when-mean-higher
       (let [test-event1 (assoc event1 :metric 40)
             test-event2 (assoc event2 :metric 42)
             test-event3 (assoc event3 :metric 43)
             conditions (assoc trigger-condition
                          :thread-through [when-higher mean]
                          :alert folds-test-bucket
                          :modifiers {:flap-threshold 1
                                             :interval 10})
             tstream (process-single-metric-rule conditions folds-test-bucket)]
         (is (= (:folds-test-bucket
                 (inject! [tstream]
                          [test-event1 test-event2 test-event3]))
                [(assoc test-event1 :service-status :problem)])))))))
