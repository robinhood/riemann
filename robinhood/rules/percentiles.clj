(ns robinhood.rules.percentiles
  (:require (riemann common
                     config
                     [streams :refer [percentiles]])
            [robinhood.rules.common :refer [aggregate-by-host-or-role]
             :rename {aggregate-by-host-or-role aggr}]))

(defn- compare-percentiles [percentile-point conditions child-stream]
  (let [interval (get-in conditions [:modifiers :interval] 300)]
    (aggr conditions
          (percentiles interval [percentile-point] child-stream))))

(def median
  (partial compare-percentiles 0.5))

(def percentile-99th
  (partial compare-percentiles 0.99))

(def percentile-95th
  (partial compare-percentiles 0.95))

