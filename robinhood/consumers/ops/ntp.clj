(ns robinhood.consumers.ops.ntp
  (:require [robinhood.notifiers.log :refer :all]
            [robinhood.notifiers.email :refer [email]]
            [robinhood.notifiers.slack :refer [slack-alert]]
            [robinhood.notifiers.opsgenie :refer [opsgenie]]
            [robinhood.rules.base :refer :all]
            [robinhood.rules.folds :refer :all]
            [robinhood.rules.hours :refer :all]
            [robinhood.rules.percentiles :refer :all]))

(def rules
  {:single-metric-rules
   [
    ;; During extended (pre and after) hours on trading days if a host has
    ;; offset exceeding +/- 45ms over a single data point. Alert right away,
    ;; don't wait for this to occur x times. This is a per host check.
    {
     :metric-name "ntpd.drift"
     :metric [-48 48]
     :type "offset"
     :roles ["order-server", "api-server",
             "consul-server"]
     :thread-through [when-out-of-range extended]
     :alert [log-warning (email "operations") (slack-alert "#operations")]
     :modifiers {:description "NTP offset"
                 :flap-threshold 1}}

    ]})
