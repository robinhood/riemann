(ns robinhood.consumers.ops.multi
  (:require [robinhood.notifiers.log :refer :all]
            [robinhood.notifiers.email :refer [email]]
            [robinhood.notifiers.opsgenie :refer [opsgenie]]
            [robinhood.notifiers.slack :refer [slack-alert]]
            [robinhood.rules.base :refer :all]
            [robinhood.rules.folds :refer :all]
            [robinhood.rules.hours :refer :all]
            [robinhood.rules.percentiles :refer :all]))

(def rules
  {:multi-metric-rules
   [
    ;; Alert when free memory, caches and buffers are all low
    {
     :filters [
               {:metric-name "proc.meminfo.memfree"
                :thread-through [mean trading]
                :modifiers {:interval 60}}
               {:metric-name "proc.meminfo.cached"
                :thread-through [mean trading]
                :modifiers {:interval 60}}
               {:metric-name "proc.meminfo.buffers"
                :thread-through [mean trading]
                :modifiers {:interval 60}}
               ]
     :metric [104857600 524288000]
     :transform max
     :thread-through [when-lower-r]
     :alert [log-warning (email "operations")]
     :modifiers {:by :host
                 :description "Available memory"}}
    ]})
