(ns robinhood.rules.folds
  (:require (riemann common
                     folds
                     [streams :refer [ddt-events ddt-real ewma
                                      moving-time-window smap]])
            [robinhood.rules.common :refer [aggregate-by aggregate-by-host-or-role]
             :rename {aggregate-by aggr
                      aggregate-by-host-or-role host-aggr}]))

(defn- fold-events [fold-fn conditions child-stream]
  (let [interval (get-in conditions [:modifiers :interval] 300)]
    (aggr conditions
          (host-aggr conditions (moving-time-window
                                 interval
                                 (smap fold-fn child-stream))))))

(def num-events
  (partial fold-events riemann.folds/count))

(def mean
  (partial fold-events riemann.folds/mean))

(def minimum
  (partial fold-events riemann.folds/minimum))

(def maximum
  (partial fold-events riemann.folds/maximum))

(def std-dev
  (partial fold-events riemann.folds/std-dev))

(def sum
  (partial fold-events riemann.folds/sum))

(defn rate [conditions child-stream]
  (let [interval (get-in conditions [:modifiers :interval] false)]
    (aggr conditions
          (host-aggr conditions
                     (if interval
                       (ddt-real interval child-stream)
                       (ddt-events child-stream))))))

(defn exp-moving-avg [conditions child-stream]
  (let [half-life (get-in conditions [:modifiers :half-life] 7)]
    (aggr conditions
          (host-aggr conditions
                     (ewma half-life child-stream)))))

