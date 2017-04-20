(ns robinhood.consumers.marketdata.trades
  (:require [robinhood.notifiers.slack :refer [slack-alert]]
            [robinhood.rules.base :refer :all]
            [robinhood.rules.hours :refer :all]))

(def rules
  {:single-metric-rules
    [
      ;; During market hours, if a box reports fewer than 100 trades per second
      ;; for 30 seconds, send an alert to slack.
      {
        :metric-name "nlsreader.recognized_trades"
        :metric [100 200]
        :value_type "count_rate"
        :thread-through [when-lower-r market]
        :alert [(slack-alert "#markets")]
        :modifiers {
          :description "NASDAQ trades per second"
          :flap-threshold 3
        }
      }

      ;; During extended hours, if a box reports no trades for 30 seconds,
      ;; send an alert to slack.
      {
        :metric-name "nlsreader.recognized_trades"
        :metric 0
        :value_type "count_rate"
        :thread-through [when-equal extended]
        :alert [(slack-alert "#markets")]
        :modifiers {
          :description "NASDAQ extended trades per second"
          :flap-threshold 3
        }
      }

    ]})
