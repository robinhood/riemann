(ns robinhood.notifiers.slack
  (:require [riemann.slack :refer :all]
            [riemann.config :refer [rh_config]]
            [clojure.string]))

(defn- format-events-for-slack [event]
  {:text (slack-escape
          (format "%s: %s is %.4g on %s"
                  (clojure.string/upper-case
                   (name (:service-status event)))
                  (:service-description event)
                  (float (:metric event 0))
                  (:host event)))
   :icon_emoji ":riemann:" })

(let [slack-config (get rh_config "slack")
      account (get slack-config "account")
      token (get slack-config "token")]
  (def slack-alert
    (memoize
     (fn [channel]
       (slack {:account account :token token}
              {:channel channel
               :username "riemann"
               :formatter format-events-for-slack})))))

