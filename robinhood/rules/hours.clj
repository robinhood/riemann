(ns robinhood.rules.hours
  (:require (riemann common
                     config
                     [streams :refer [call-rescue expired?]]
                     [test :refer [tap io inject! deftest]])
            [cheshire.core :refer [parse-string]]
            [clj-time.core :as t]
            [clj-http.client :as client]
            [clj-time.coerce :refer [from-date from-string]]
            [clj-time.format :refer [unparse with-zone formatter formatters]]
            [clj-time.predicates :refer [weekend? weekday?]]))

(defn event-datetime [event]
  (t/to-time-zone (from-date (riemann.common/time-at (:time event)))
                  (t/default-time-zone)))

(defn- convert-market-status-to-dt [status]
  (if (status "is_open")
    (into {:open? true}
          (for [[k v] status :when (clojure.string/ends-with? k "_at")]
            [(keyword k) (clj-time.format/parse v)]))
    {:open? false}))

(def get-market-status
  "Returns a json of market datetime objects for the given date"
  (memoize
   (fn [date]
     (let [query-url "https://api.robinhood.com/markets/XNYS/hours/"]
       (-> (client/get (str query-url date) {:accept :json})
           :body
           parse-string
           convert-market-status-to-dt)))))

(def i-market-hours
  "Interval for market hours"
  (memoize
   (fn [date]
     (let [all-market-hours (get-market-status date)]
       (if (:open? all-market-hours)
         (t/interval (:opens_at all-market-hours)
                     (:closes_at all-market-hours)))))))

(def i-pre-market-hours
  "Interval for pre-market hours"
  (memoize
   (fn [date]
     (let [all-market-hours (get-market-status date)]
       (if (:open? all-market-hours)
         (t/interval (:extended_opens_at all-market-hours)
                     (:opens_at all-market-hours)))))))

(def i-after-market-hours
  "Interval for after-market hours"
  (memoize
   (fn [date]
     (let [all-market-hours (get-market-status date)]
       (if (:open? all-market-hours)
         (t/interval (:closes_at all-market-hours)
                     (:extended_closes_at all-market-hours)))))))

(def i-extended-hours
  "Interval for extended hours"
  (memoize
   (fn [date]
     (let [all-market-hours (get-market-status date)]
       (if (:open? all-market-hours)
         (t/interval (:extended_opens_at all-market-hours)
                     (:extended_closes_at all-market-hours)))))))

(def i-trading-day
  "Interval for FULL trading day (24 hours)"
  (memoize
   (fn [date]
     (let [all-market-hours (get-market-status date)
           day-start (t/from-time-zone (from-string date) (t/default-time-zone))]
       (if (:open? all-market-hours)
         (t/interval day-start (t/plus day-start (t/hours 24))))))))

(def i-non-trading-day
  "Interval active when markets are closed"
  (memoize
   (fn [date]
     (let [all-market-hours (get-market-status date)
           day-start (t/from-time-zone (from-string date) (t/default-time-zone))]
       (if (not (:open? all-market-hours))
         (t/interval day-start (t/plus day-start (t/hours 24))))))))

(let [interval-defns {:market i-market-hours
                      :pre-market i-pre-market-hours
                      :after-market i-after-market-hours
                      :extended i-extended-hours
                      :trading i-trading-day
                      :non-trading i-non-trading-day}]
  (defn- check-event-hours
    "Given a trading hour type (:market, :extended etc), call children if the
     event happened during that interval."
    [trading-hour-type _ & children]
    (fn [event]
      (if (not (expired? event))
        (let [event-time (event-datetime event)
              date (unparse
                    (with-zone (formatters :date) (t/default-time-zone)) event-time)
              market-status (get-market-status date)
              interval ((trading-hour-type interval-defns) date)]
          (if (and interval
                   (t/within? interval event-time))
            (call-rescue event children)))))))

(def market
  "Active only during market hours"
  (partial check-event-hours :market))

(def pre-market
  "Active only during pre-market"
  (partial check-event-hours :pre-market))

(def after-market
  "Active only during after hours"
  (partial check-event-hours :after-market))

(def extended
  "Active only during extended hours (market hours + pre market + after hours"
  (partial check-event-hours :extended))

(def trading
  "Active only during trading days (markets are open)"
  (partial check-event-hours :trading))

(def non-trading
  "Active only during non trading days (markets are closed)"
  (partial check-event-hours :non-trading))

(defn weekends [_ & children]
  "Active only during the weekends"
  (fn [e]
    (if (weekend? (event-datetime e))
      (call-rescue e children))))

(defn weekdays [_ & children]
  "Active only during the weekdays"
  (fn [e]
    (if (weekday? (event-datetime e))
      (call-rescue e children))))

(defn always [_ & children]
  "Passes events at all hours"
  (fn [e]
    (call-rescue e children)))
