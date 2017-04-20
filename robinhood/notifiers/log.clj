(ns robinhood.notifiers.log
  (:require [clojure.tools.logging :refer [log]]))

(defn- log-stuff [level stuff]
  (log level nil (clojure.core/print-str stuff)))

(def log-warning (partial log-stuff :warn))
(def log-info (partial log-stuff :info))
(def log-debug (partial log-stuff :debug))
