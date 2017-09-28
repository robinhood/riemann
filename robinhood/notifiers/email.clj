(ns robinhood.notifiers.email
  (:require [riemann.email :refer :all]
            [riemann.config :refer [rh_config]]
            [clojure.string]))

(defn format-subject [events]
  (let [subjects (map (fn [event]
                        (format "%s: %s is %.4g on %s"
                                (clojure.string/upper-case
                                 (name (:service-status event)))
                                (:service-description event)
                                (float (:metric event 0))
                                (:host event))) events)]
    (clojure.string/join ", " subjects)))

(def get-mailer
  (memoize
   (fn [config]
     (mailer {:host (get config "aws_mail_server")
              :port 587
              :user (get config "aws_mail_user")
              :pass (get config "aws_mail_pass")}
             {:from (get config "default_from_address")
              :subject format-subject}))))

(def email
  (memoize
   (fn [target]
     (let [mailer (get-mailer rh_config)
           domain (get-in rh_config ["emails" "domain"])
           target-address (get-in rh_config ["emails" target] target)]
       (if (clojure.string/ends-with? target-address domain)
         (mailer target-address)
         (mailer (str target-address domain)))))))

