(ns robinhood.consumers.ops.metrics
  (:require [robinhood.notifiers.email :refer [email]]
            [robinhood.notifiers.slack :refer [slack-alert]]
            [robinhood.notifiers.opsgenie :refer [opsgenie]]
            [robinhood.rules.base :refer :all]
            [robinhood.rules.folds :refer :all]
            [robinhood.rules.hours :refer :all]
            [robinhood.rules.percentiles :refer :all]))

(def rules
  {:single-metric-rules
   [
    ;; Alert if the p99 for secrets read is above 20ms
    {
     :metric-name "tsd.hbase.rpcs"
     :metric 5000
     :type "append"
     :thread-through [when-lower rate]
     :alert [(email "ops")]
     :modifiers {:description "OpenTSDB HBase writes"
                 :watch-events true
                 :by-host true}}
    ]})
