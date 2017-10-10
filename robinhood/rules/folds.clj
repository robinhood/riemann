(ns robinhood.rules.folds
  (:require (riemann common
                     folds
                     [streams :refer [ddt-events ddt-real ewma
                                      moving-time-window smap]])
            [robinhood.rules.common :refer [aggregate-by]
             :rename {aggregate-by aggr}]))

(defn- fold-events [fold-fn conditions child-stream]
  (let [interval (get-in conditions [:modifiers :interval] 300)]
    (aggr conditions
          (moving-time-window
           interval
           (smap #(sort-by :time > %) (smap fold-fn child-stream))))))

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

(defn- unique-event-count [count-key events]
  (let [events (remove nil? events)]
    (if-let [e (first events)]
      (assoc e :metric
             (clojure.core/count
              (remove nil? (into #{} (map count-key events)))))
      (riemann.common/event {:metric 0}))))

(defn num-unique-events [conditions child-stream]
  (let [event-counter (partial unique-event-count
                               (get-in conditions [:modifiers :count-key]))
        interval (get-in conditions [:modifiers :interval] 300)]
    (aggr conditions
          (moving-time-window
           interval
           (smap #(sort-by :time > %) (smap event-counter child-stream))))))

(defn rate [conditions child-stream]
  (let [interval (get-in conditions [:modifiers :interval] false)]
    (aggr conditions
          (if interval
            (ddt-real interval child-stream)
            (ddt-events child-stream)))))

(defn exp-moving-avg [conditions child-stream]
  (let [half-life (get-in conditions [:modifiers :half-life] 7)]
    (aggr conditions
          (ewma half-life child-stream))))

