(ns robinhood.notifiers.common
  (:require (riemann config
                     [streams :refer [by changed runs smap sdo where* with]]
                     [test :as test])
            [robinhood.notifiers.elasticsearch :as es]
            [robinhood.notifiers.log :as l]))

(defn disseminate-check-output [conditions]
  (let [children (apply sdo (:alert conditions))
        modifiers (:modifiers conditions {})
        samples (:flap-threshold modifiers 3)
        rewrite-service (fn [e]
                          (if-let [new-description (:description modifiers)]
                            (assoc e :service new-description)
                            e))
        actionable-service-status? (fn [e] (or (= (:service-status e) :ok)
                                               (= (:service-status e) :problem)))
        detector (smap rewrite-service
                       (by :host
                           (runs samples :service-status
                                 (test/io es/push-to-es)
                                 (changed :service-status
                                          riemann.config/index)
                                 (where* actionable-service-status?
                                          (changed :service-status {:init :ok}
                                                   children)))))]
    (fn [e] (detector e))))

