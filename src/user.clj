(ns user
  (:require [cljka.kafka :as kafka]
            [cljka.config :refer [load-config]]
            [clojure.repl :refer [dir-fn]]))

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

(defn reload-config!
  "Reloads the configuration from file."
  []
  (reset! config (load-config)))

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
   (->> ['kafka 'user]
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
           (str "(" (cyan (str (name ns) "/" (:name f))) " " (yellow arglist) ")")))
       (println (green (:doc f)))
       (println)))

   (println)))

(reload-config!)

;
;--- Prompt
;

(println
  (str
    (yellow "Welcome to the Kafka Tooling REPL. Type ")
    (magenta "(help)")
    (yellow " or ")
    (magenta "(help '<ns>)")
    (yellow " for more information.")))