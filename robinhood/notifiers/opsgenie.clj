(ns robinhood.notifiers.opsgenie
  (:require [riemann.config :refer [rh_config]]
            [clj-http.client :as client]
            [cheshire.core :as json]
            [riemann.streams :refer [where*]]
            [robinhood.rules.common :refer [service-okay?]]))

;; copied (and modified)from upstream opsgenie, since we have a
;; different meaning for :state.

(def ^:private alerts-url
  "https://api.opsgenie.com/v1/json/alert")

(defn- post
 "Post to OpsGenie"
 [url body]
  (client/post url
                {:body body
                 :socket-timeout 5000
                 :conn-timeout 5000
                 :content-type :json
                 :accept :json
                 :throw-entire-message? true}))

(defn- message
  "Generate description based on event.
  Because service might be quite long and opsgenie limits message, it
  pulls more important info into beginning of the string"
  [event]
  (format "%s is %.4g on %s"
          (:service-description event)
          (float (:metric event 0))
          (:host event)))

(defn- description
  "Generate message based on event"
  [event]
  (str
   "Host: " (:host event)
   " \nService: " (:metric-name event)
   " \nState: " (:service-status event)
   " \nMetric: " (:metric event 0)
   " \nDescription: " (:service event)))

(defn- api-alias
  "Generate OpsGenie alias based on event"
  [event]
  (hash (str (:host event) \uffff (:service event))))

(defn- create-alert
  "Create alert in OpsGenie"
  [api-key event recipients]
  (post alerts-url (json/generate-string
                    {:message (message event)
                     :description (description event)
                     :apiKey api-key
                     :alias (api-alias event)
                     :tags (clojure.string/join "," (:tags event))
                     :recipients recipients})))
(defn- close-alert
  "Close alert in OpsGenie"
  [api-key event]
  (post (str alerts-url "/close")
        (json/generate-string
          {:apiKey api-key
           :alias (api-alias event)})))

(defn opsgenie-adapter
  "Creates an OpsGenie adapter. Takes your OG service key, and returns a map of
  functions which trigger and resolve events. clojure/hash from event host, service and tags
  will be used as the alias.

  (let [og (opsgenie \"my-service-key\" \"recipient@example.com\")]
    (changed-state
      (where (state \"ok\") (:resolve og))
      (where (state \"critical\") (:trigger og))))"
  [service-key recipients]
  {:trigger     #(create-alert service-key % recipients)
   :resolve     #(close-alert service-key %)})

(defn- opsgenie-stream [api-key recepients]
  (let [og (opsgenie-adapter api-key (get-in rh_config "opsgenie" recepients))]
    (where* service-okay? (:resolve og)
            (else (:trigger og)))))

(def opsgenie
  (partial opsgenie-stream (get rh_config "opsgenie_api_key")))
