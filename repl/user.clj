(ns user
  (:require [cljka.config :refer [load-config]]
            [clojure.repl :refer [dir-fn]]
            [taoensso.timbre :refer [set-min-level!]]
            [cljka.core :refer :all]
            [cljka.channel :refer :all]
            [clojure.core.async :as async]))

(set-min-level! :warn)

;
;--- Colourisation
;

(defn- escape-code [i] (str "\033[" i "m"))
(defn- colourise [c] (fn [s] (str c s (escape-code 0))))

(def ^:private green (colourise (escape-code 32)))
(def ^:private yellow (colourise (escape-code 33)))
(def ^:private magenta (colourise (escape-code 35)))
(def ^:private cyan (colourise (escape-code 36)))

;
;--- Cached configuration
;

(def ^:no-doc config (atom nil))

(defn reload!
  "Reloads the configuration from file."
  []
  (reset! config (load-config)))

(reload!)

;
;--- Help
;

(defn- ^:no-doc get-namespace-functions
  [ns]
  (->> (dir-fn ns)
       (map name)
       (sort)
       (map #(-> (symbol (name ns) %)
                 resolve
                 meta))))

(defn ^:no-doc help
  ([]
   (->> ['cljka.core 'cljka.channel]
        (map help)
        vec)
   :done)
  ([ns]
   (println "----------------------")
   (println (magenta (name ns)))
   (println "----------------------")
   (println)

   (doseq [f (get-namespace-functions ns)]
     (when-not (:no-doc f)
       (doseq [arglist (:arglists f)]
         (println
           (str "(" (cyan (:name f)) " " (yellow arglist) ")")))
       (println (green (:doc f)))
       (println)))

   (println)))

;
;--- Prompt
;

(println
  (str
    (yellow "Welcome to the Kafka Tooling REPL. Type ")
    (magenta "(help)")
    (yellow " for more information.")))