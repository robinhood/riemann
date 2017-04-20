(ns robinhood.tests.hours
  (:require (riemann [test :refer [tap io inject! deftest]])))

(riemann.test/tests

 (require '[riemann.test :refer [tap io inject! deftest]]
          '[riemann.config :refer [streams]]
          '[clojure.test.check :as tc]
          '[clojure.test.check.clojure-test :refer [defspec]]
          '[clojure.test.check.generators :as gen]
          '[clojure.test.check.properties :as prop]
          '[clj-time.core :as t]
          '[clj-time.format :refer [unparse with-zone formatter formatters]]
          '[robinhood.rules.hours :refer [get-market-status weekdays weekends
                                          extended after-market pre-market
                                          always trading non-trading market
                                          event-datetime]])

 (let [hours-test-bucket (tap :hours-test-bucket riemann.streams/bit-bucket)
       current-ts (int (riemann.time/unix-time-real))
       one-year (* 60 60 24 365)]

   (defn count-passing-streams [test-streams event]
     (let [desired-output [event]]
       (count (filter #(= (:hours-test-bucket (inject! [%] [event]))
                          desired-output)
                      test-streams))))

   (defspec generative-hours-tests 10
     (prop/for-all [event-ts (gen/large-integer* {:min (- current-ts one-year)
                                                  :max (+ current-ts one-year)})]
                   (let [event {:time event-ts}
                         et (event-datetime event)
                         date (unparse (with-zone (formatters :date)
                                         (t/default-time-zone)) et)
                         market-status (get-market-status date)
                         always-stream (always {} hours-test-bucket)
                         weekend-stream (weekends {} hours-test-bucket)
                         weekday-stream (weekdays {} hours-test-bucket)
                         trading-stream (trading {} hours-test-bucket)
                         non-trading-stream (non-trading {} hours-test-bucket)
                         extended-stream (extended {} hours-test-bucket)
                         pre-market-stream (pre-market {} hours-test-bucket)
                         after-market-stream (after-market {} hours-test-bucket)
                         market-stream (market {} hours-test-bucket)]

                     ;; Always pass this event through
                     (= (count-passing-streams [always-stream] event) 1)

                     ;; Every event occours either on a trading day or
                     ;; a market holiday
                     (= (count-passing-streams [trading-stream
                                                non-trading-stream] event) 1)

                     ;; Every event that happened on a trading day,
                     ;;   1. happens either before or after extended trading hours (or)
                     ;;   2. happens during extended trading hours, it happens
                     ;;      during pre-market, market or during after hours
                     ;; If none of the above conditions hold, the event falls on a
                     ;; trading holiday.
                     (or (and (= (count-passing-streams [trading-stream] event) 1)
                              (or (= (count-passing-streams [extended-stream
                                                             pre-market-stream
                                                             market-stream
                                                             after-market-stream]
                                                            event) 2)
                                  (or (t/before? et (:extended_opens_at market-status))
                                      (t/after? et (:extended_closes_at market-status)))))
                         (= (count-passing-streams [non-trading-stream] event) 1)))))))
