(ns cljka.config
  (:require [clojure.edn :as edn]))

(defn load-config
  "Reloads the cljka configuration file."
  []
  (-> (str (System/getProperty "user.home") "/.config/cljka/config.edn")
      slurp
      edn/read-string))
