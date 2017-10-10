(ns robinhood.notifiers.common
  (:require (riemann config
                     [streams :refer [by changed expired? runs smap sdo where* with]]
                     [test :as test])
            [robinhood.rules.common :refer [aggregate-by]
             :rename {aggregate-by aggr}]
            [robinhood.notifiers.elasticsearch :as es]
            [robinhood.notifiers.log :as log]))


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


(defn actionable-service-status? [e]
  (or (= (:service-status e) :ok)
      (= (:service-status e) :problem)))


(defn rewrite-host [conditions e]
  (let [by-flags (get-in conditions [:modifiers :by] :rule-id)
        host-adjusted-event (assoc e :host (:roles e))
        generic-event (assoc e :host "generic" :roles "generic")]
    (if (sequential? by-flags)
      (if (some {:host true} by-flags)
        e
        (if (some {:roles true} by-flags)
          host-adjusted-event
          generic-event))
      (if (= by-flags :host)
        e
        (if (= by-flags :roles)
          host-adjusted-event
          generic-event)))))


(defn rewrite-service [conditions e]
  (let [by-flags (get-in conditions [:modifiers :by])
        index-suffix (if (sequential? by-flags)
                       (clojure.string/join (map #(get e % "") by-flags))
                       (get e by-flags ""))
        rule-identifier (:rule-id conditions)]
    (if (clojure.string/starts-with? (:service e) rule-identifier)
      e
      (assoc e :service (str rule-identifier index-suffix)))))


(defn rewrite-status [e]
  (when (actionable-service-status? e)
    (if (= "expired" (:state e))
      (assoc e :service-status :dead)
      e)))


(defn make-event-watcher
  "Makes service watcher that complains if it starts to get expired
  events (from the index)"
  [conditions]
  (let [children (apply sdo (:alert conditions))
        rule-identifier (:rule-id conditions)
        rewrite-event (comp (partial rewrite-service conditions)
                            rewrite-status)]
    ; only allows events that passed through rules that created us.
    (where* #(= (:rule-id %) rule-identifier)
            ;; #(log/log-warning (str "Watcher got: " (clojure.core/print-str %)))
            (smap rewrite-event
                  (where* expired?
                          (test/io es/push-to-es)
                          (else riemann.config/index))
                  (by [:host :service]
                      (changed actionable-service-status? {:init true}
                               children))))))


(defn problem-detector [conditions]
  (let [children (apply sdo (:alert conditions))
        samples (get-in conditions [:modifiers :flap-threshold] 3)]
    (runs samples :service-status
          (test/io es/push-to-es)
          (where* actionable-service-status?
                  (changed :service-status {:init :ok}
                           children)))))


(defn prep-output-streams [conditions]
  (let [modifiers (:modifiers conditions {})
        rule-identifier (:rule-id conditions)
        rule-ttl (get-in conditions [:modifiers :ttl]
                          (riemann.config/rh_config "default_ttl"))
        add-service-description (fn [e]
                                  (if-let [new-description (:description modifiers)]
                                    (assoc e
                                      :service-description (str-interpolate new-description e))
                                    e))
        event-watcher (if (:watch-events modifiers false)
                          (let [watcher-stream (make-event-watcher conditions)]
                            (swap! riemann.config/watcher-streams conj watcher-stream)
                            watcher-stream)
                          identity)
        write-to-output-streams (with {:ttl rule-ttl :rule-id rule-identifier}
                                      (where* (complement expired?)
                                              (smap add-service-description
                                                    event-watcher
                                                    (aggr conditions
                                                           (problem-detector conditions)))))]
    (fn [e]
      (write-to-output-streams ((partial rewrite-host conditions) e)))))

