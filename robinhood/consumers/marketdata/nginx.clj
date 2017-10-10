(ns robinhood.consumers.marketdata.nginx
  (:require [robinhood.notifiers.log :refer :all]
            [robinhood.notifiers.email :refer [email]]
            [robinhood.notifiers.opsgenie :refer [opsgenie]]
            [robinhood.notifiers.slack :refer [slack-alert]]
            [robinhood.rules.base :refer :all]
            [robinhood.rules.folds :refer :all]
            [robinhood.rules.hours :refer :all]
            [robinhood.rules.percentiles :refer :all]))

(defn http-err-rate [r2xx r4xx r5xx]
  (* 100 (/ r5xx (+ r2xx r4xx r5xx))))

(def rules
  {:multi-metric-rules
   [
    ;; percentage of 500 status code to the other status codes for
    ;; marketdata-server
    {
     :filters [
               {:metric-name "nginx.hits.rate"
                :status "2xx"
                :roles ["marketdata-server"]
                :thread-through [mean sum]
                :modifiers {:by :roles
                            :interval 60}}
               {:metric-name "nginx.hits.rate"
                :status "4xx"
                :roles ["marketdata-server"]
                :thread-through [mean sum]
                :modifiers {:by :roles
                            :interval 60}}

               {:metric-name "nginx.hits.rate"
                :status "5xx"
                :roles ["marketdata-server"]
                :thread-through [mean sum]
                :modifiers {:by :roles
                            :interval 60}}
               ]
     :metric [2 5]
     :transform http-err-rate
     :thread-through [when-higher-r]
     :alert [log-warning (email "markets") (slack-alert "#markets")]
     :modifiers {:description "Marketdata HTTP error percentage"
                 :by :roles}}
    ]})
