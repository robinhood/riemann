(ns robinhood.notifiers.common
  (:require (riemann config
                     [streams :refer [by changed expired? runs smap sdo where* with]]
                     [test :as test])
            [robinhood.rules.common :refer [aggregate-by]
             :rename {aggregate-by aggr}]
            [robinhood.notifiers.elasticsearch :as es]))


; stolen from https://github.com/TheClimateCorporation/lemur/blob/master/src/main/clj/lemur/evaluating_map.clj
(defn str-interpolate
  "Replace 'variables' in s with the correspondig map value from value-map. Variables
  are encoded into the string inside {}, for example
    \"ab{letter}def\"
  with value-map
    { :letter \"c\" }
  would yield
    \"abcdef\"
  Note that the value-map keys should be keywords."
  [s value-map]
  (let [get-tag-value (fn [[_ v]] (str ((keyword v) value-map)))]
    (clojure.string/replace s #"\{([^\}]*)\}" get-tag-value)))


(defn make-event-watcher
  "Makes service watcher that complains if it starts to get expired
  events (from the index)"
  [conditions]
    (let [children (apply sdo (:alert conditions))
          rule-identifier (:rule-id conditions)
          actionable-service-status? (fn [e] (or (= (:service-status e) :ok)
                                                 (= (:service-status e) :problem)))
          event-ttl (get-in conditions [:modifiers :ttl]
                            (riemann.config/rh_config "default_ttl"))
          rewrite-status (fn [e] (when (actionable-service-status? e)
                                   (if (= "expired" (:state e))
                                     (assoc e :service-status :dead)
                                     e)))]
      ; only allows events that passed through rules that created us.
      (where* #(= (:rule-id %) rule-identifier)
              (by [:host :service]
                  (smap rewrite-status
                        (where* expired?
                                (test/io es/push-to-es)
                                (else (with :ttl event-ttl riemann.config/index)))
                        (changed actionable-service-status? {:init true}
                                 children))))))


(defn disseminate-check-output [conditions]
  (let [children (apply sdo (:alert conditions))
        modifiers (:modifiers conditions {})
        samples (:flap-threshold modifiers 3)
        rule-identifier (str (java.util.UUID/randomUUID))
        rewrite-service (fn [e]
                          (if-let [new-description (:description modifiers)]
                            (assoc e
                              :service-description (str-interpolate new-description e)
                              :rule-id rule-identifier)
                            (assoc e :rule-id rule-identifier)))
        actionable-service-status? (fn [e] (or (= (:service-status e) :ok)
                                               (= (:service-status e) :problem)))
        event-watcher (if (:watch-events modifiers false)
                          (let [watcher-stream (make-event-watcher
                                                (assoc conditions
                                                  :rule-id rule-identifier))]
                            (swap! riemann.config/watcher-streams conj watcher-stream)
                            watcher-stream)
                          identity)
        detector (smap rewrite-service
                       (aggr conditions
                             (sdo
                              event-watcher
                              (by :host
                                  (runs samples :service-status
                                        (test/io es/push-to-es)
                                        (where* actionable-service-status?
                                                (changed :service-status {:init :ok}
                                                         children)))))))]
    (fn [e] (detector e))))

