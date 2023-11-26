(ns cljka.confirm
  (:require [clojure.string :refer [upper-case]]))

(defn confirmed?
  [prompt]
  (println prompt)
  (flush)
  (println "Continue? (y/N)")
  (flush)
  (Thread/sleep 500)
  (let [result (read-line)]
    (= "Y" (upper-case result))))

(defmacro with-confirmation
  [prompt & body]
  `(if (confirmed? ~prompt)
     ~@body
     (throw (Exception. "Operation cancelled."))))
