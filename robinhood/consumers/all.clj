(ns robinhood.consumers.all
  (:require [robinhood.consumers.processor :as processor]
            [clojure.string :as string]))

;; Add your consumer ns here
(require 'robinhood.consumers.marketdata.nginx)
(require 'robinhood.consumers.marketdata.trades)
(require 'robinhood.consumers.ops.ntp)
(require 'robinhood.consumers.ops.multi)

(def all-streams
  (filter
   boolean
   (for [lib (loaded-libs)]
     (when (and (ns-resolve lib 'rules)
                (string/starts-with? (name lib)
                                     "robinhood.consumers"))
       (doall
        (map processor/process-rules (var-get (ns-resolve lib 'rules))))))))

