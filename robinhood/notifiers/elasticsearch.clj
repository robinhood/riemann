(ns robinhood.notifiers.elasticsearch
  (:require [clojure.string :refer [join]]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [clj-time.core]
            [clj-time.coerce :as time-coerce]
            [clj-time.format :as time-format]
            [riemann.streams]
            [robinhood.notifiers.log :as log]
            [riemann.config :refer [async-queue!]]))

(defn- datetime-from-event
  "Returns the datetime from event correcting (secs -> millisecs) before conversion."
  [event]
  (time-coerce/from-long (long (* 1000 (:time event)))))

(defn- post
  "POST to Elasticsearch."
  [esindex formatted-events]
  (let [http-options {:body (str (join "\n" formatted-events) "\n")
                      :content-type :text
                      :conn-timeout 5000
                      :socket-timeout 5000
                      :throw-entire-message? true}]
    (when (seq formatted-events)
      (log/log-info (str "Pushing " (/ (count formatted-events) 2) " to ES."))
      (http/post esindex http-options))))

;;
;; from http://stackoverflow.com/questions/34379009/different-values-for-md-5-hash-depending-on-technique
;;
(defn- hash-string
  "Use java interop to flexibly hash strings"
  [string algo base]
  (let [hashed
        (doto (java.security.MessageDigest/getInstance algo)
          (.reset)
          (.update (.getBytes string)))]
    (format "%032x" (new java.math.BigInteger 1 (.digest hashed)))))

(defn- hash-md5
  "Generate a md5 checksum for the given string"
  [string]
  (hash-string string "MD5" 16))

(defn- format-opentsdb-event
  "Formats an event for Elasticsearch, drops \"description\" and re-formats \"time\"."
  [event]
  {:host (:host event)
   :service (:service event)
   :metric (:metric event 0)
   :tags (:tags event)
   :service-status (:service-status event)
   (keyword "@timestamp") (time-format/unparse
                           (time-format/formatters :date-time)
                           (time-coerce/from-long (long (* 1000 (:time event)))))})

(defn- make-es-bulk-event-header [event]
  (conj
   {"index" (conj {"_id" (hash-md5 (str (:service event)
                                        (:host event "")))}
                  (if (:service-status event)
                    ["_type" "check"]
                    ["_type" "event"]))}))

(defn elasticsearch
  "Returns a function which accepts events and sends them to  Elasticsearch."
  [event-formatter]
  (let [es-endpoint "http://127.0.0.1:9200"
        es-index "riemann"
        format-es-batch (fn [event]
                          (if (< (rand) (:es_log_probability event 1))
                            [(make-es-bulk-event-header event)
                             (event-formatter event)]))]
    (fn [events]
      (let [riemann-events (filter :service-status events)
            normal-events (filter (complement :service-status) events)]
        (post
         (format "%s/%s%s/_bulk"
                 es-endpoint
                 es-index
                 "-checks")
         (map json/generate-string
              (flatten (remove nil? (map format-es-batch riemann-events)))))
        (post
         (format "%s/%s%s/_bulk"
                 es-endpoint
                 es-index
                 "-events")
         (map json/generate-string
              (flatten (remove nil? (map format-es-batch normal-events)))))))))

(let [async-push-to-es (async-queue!
                        :es-logger
                        {:queue-size     1e8  ; 100M events max
                         :core-pool-size 2    ; Minimum 2 threads
                         :max-pools-size 8}   ; Maxium 8 threads
                        (riemann.streams/batch 10e3 1 ; push every 10k
                                                     ; events or every
                                                     ; second,
                                                     ; whichever
                                                     ; happens first
                                               (elasticsearch format-opentsdb-event)))]
  (def push-to-es async-push-to-es))

