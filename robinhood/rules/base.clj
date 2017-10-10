(ns robinhood.rules.base
  (:require (riemann common
                     config
                     [streams :refer [where* with]])
            [robinhood.rules.common :refer [aggregate-by]
             :rename {aggregate-by aggr}]))

(defn compare-metric-and-conditions
  "Checks if the event satisfies additional conditions and then checks
   if metric triggers the condition"
  [metric-comparison-fn conditions child-stream]
  (let [metric-checker (fn [event]
                         (metric-comparison-fn (:metric event)
                                               (:metric conditions)))]
    (where* metric-checker
            (with {:service-status :problem} child-stream)
            (else
             (with {:service-status :ok} child-stream)))))

(defn compare-metric-and-conditions-r
  "Checks if the event satisfies additional conditions and then checks
   if metric triggers the condition"
  [metric-comparison-fn-p metric-comparison-fn-w conditions child-stream]
  (let [metric-checker-p (fn [event]
                           (metric-comparison-fn-p (:metric event)
                                                   (:metric conditions)))
        metric-checker-w (fn [event]
                           (metric-comparison-fn-w (:metric event)
                                                   (:metric conditions)))]
    (where* metric-checker-p
            (with {:service-status :problem} child-stream)
            (else
             (where* metric-checker-w
                     (with {:service-status :warning} child-stream)
                     (else
                      (with {:service-status :ok} child-stream)))))))

(def when-in-range
  "Fires when metric value lies in the given range"
  (partial compare-metric-and-conditions
           (fn [event-value [min max]]
             (<= min event-value max))))

(def when-out-of-range
  "Fires when metric value lies outside the given range"
  (partial compare-metric-and-conditions
           (fn [event-value [min max]]
             (or (< event-value min)
                 (> event-value max)))))

(def when-higher
  "Fires when metric value is greater than the condition value"
  (partial compare-metric-and-conditions >))

(def when-lower
  "Fires when metric value is greater than the condition value"
  (partial compare-metric-and-conditions <))

(def when-higher-r
  "Fires when metric value is greater than the higher condition bound,
   when metric value is higher than lower bound, this goes into a
   warning stage"
  (partial compare-metric-and-conditions-r
           (fn [event-value [low high]]
             (> event-value high))
           (fn [event-value [low high]]
             (> event-value low))))

(def when-lower-r
  "Fires when metric value is lower than the condition value"
  (partial compare-metric-and-conditions-r
           (fn [event-value [low high]]
             (< event-value low))
           (fn [event-value [low high]]
             (< event-value high))))

(def when-equal
  "Fires when metric value is equal to the condition value"
  (partial compare-metric-and-conditions ==))

(def when-not-equal
  "Fires when metric value is not equal to the condition value"
  (partial compare-metric-and-conditions (complement ==)))

