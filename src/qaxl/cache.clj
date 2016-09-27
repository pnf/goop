(ns qaxl.cache
  (:require [clojure.core.cache :refer (soft-cache-factory)]))

(def cache (atom (soft-cache-factory {})))
(defn soft-memoize
  "Same as clojure.core/memoize, but uses a soft cache"
  {:static true}
  [f]
  (fn [& args]
    (if-let [e (find @cache (conj args f))]
      (val e)
      (let [ret (apply f args)]
        (swap! cache assoc (conj args f) ret)
        ret))))

