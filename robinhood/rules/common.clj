(ns robinhood.rules.common
  (:require (riemann common
                     config
                     [streams :refer [where* by smap expired?]])
            [clojure.string]
            [robinhood.notifiers.log :as log]))

(defn tag-event [e tag value]
  (if (not (tag e))
    (assoc e tag value)
    e))

(defn service-okay? [e]
  (= (:service-status e) :ok))

(defn get-regex-keys [conditions]
  (keys
   (filter (fn [[k v]] (isa? java.util.regex.Pattern (type v)))
           conditions)))

(defn sanitize-conditions [conditions]
  (dissoc conditions
          :alert
          :filters
          :metric
          :modifiers
          :roles
          :thread-through))

(defn rewrite-host [e]
  (assoc e :host (clojure.string/join "," (:tags e))))

(defmacro aggregate-by-host-or-role
  [conditions consumer-stream]
  `(let [aggregate-by-host# (by :host ~consumer-stream)]
     (if (get-in ~conditions [:modifiers :by-host] true)
       aggregate-by-host#
       (smap rewrite-host aggregate-by-host#))))

(defmacro aggregate-by
  [conditions consumer-stream]
  `(if-let [aggregator# (get-in ~conditions [:modifiers :by])]
     (by aggregator# ~consumer-stream)
     ~consumer-stream))

(defn subset? [conditions event]
  (every? (fn [[k v]] (= (k event) v)) conditions))

(defn check-event-tags
  "Checks if the service name, and any of the roles (tags) and other
   specified conditions are present in the event"
  ([{roles :roles, :or {roles []} :as conditions} event]
     (check-event-tags roles
                       (get-in conditions [:modifiers :exclude-roles] [])
                       (sanitize-conditions conditions) event))

  ([roles excluded-roles conditions event]
     (and (or (empty? roles)
              (riemann.streams/tagged-any? roles event))
          (or (empty? excluded-roles)
              (not (riemann.streams/tagged-any? excluded-roles event)))
          (subset? conditions event))))

(defn block-expired-events
  "Filters out expired events"
  [_ child-stream]
  (where* (complement expired?)
          child-stream))

(defn filter-events
  [conditions child-stream]
  (let [roles (:roles conditions [])
        exclude-roles (get-in conditions [:modifiers :exclude-roles] [])
        sanitized-conditions (sanitize-conditions conditions)]
    (where* (partial check-event-tags roles exclude-roles sanitized-conditions)
            child-stream)))

