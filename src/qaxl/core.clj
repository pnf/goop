(ns qaxl.core
  (:use clojure.walk clojure.pprint qaxl.cache)
  (:require ;[co.paralleluniverse.pulsar.async :as qa]
            [clojure.core.async :as a]
            [co.paralleluniverse.pulsar.core :as q]
            [clojure.test :refer [function?]]
            [co.paralleluniverse.fiber.httpkit.client :as hk]
            [co.paralleluniverse.pulsar.core :refer [fiber]]
            [clojure.core.match :refer [match]]))


(declare parallelize)

(defmacro qaxl
  "Parallelize and evaluate an expression"
  [form]
  (:form (parallelize form)))

(defn- res= [a b]
  (or (= a b)
      (let [ra (resolve a)
            rb (resolve b)]
        (and ra rb (= (resolve a) (resolve b))))))

;; If there's a channel that will return the value of this symbol, substitute its deref.
(defn- parallelize-subst [s s2c]
  (if (symbol? s)
    (if (contains? s2c s)
      {:form `(deref ~(get s2c s)) :s2c s2c :par true}
      {:form s :susp (some-> s resolve var-get q/suspendable?) :s2c s2c})
    {:form s :s2c s2c}))

(defn- parallelize-forms [forms s2c] (map #(parallelize % s2c) forms))

(defn- par-to-bindings [ps] ;; => [bs args]
  (let [[bs args]
        (reduce
         (fn [[bs args] p]
           (if (:par p)
             (let [ch (gensym "p")]
               [(concat bs [ch `(q/promise (fn [] ~(:form p)))])
                (conj args `(deref ~ch))])
             [bs (conj args (:form p))]))
         [[] []]
         ps)]
    [bs (seq args)]))


(defn- parallelize-func [forms s2c]
  (when (:trace s2c) (println "parallelize-func" forms))
  (let [ps         (parallelize-forms forms s2c)
        [bs forms] (par-to-bindings ps)
        par        (or (some :par ps) (:susp (first ps)))]
    {:form (if (seq bs) `(let [~@bs] (~@forms)) `(~@forms)) :par par :s2c s2c}))

;; Possibly lazy
(defn- parallelize-special [[s & forms] s2c]
  (when (:trace s2c) (println "parallelize-special" s forms))
  (let [ps (parallelize-forms forms s2c)]
    {:form `(~s ~@(map :form ps)) :par (some :par ps) :s2c s2c}))

(defn- parallelize-coll [form s2c]
  (let [ps  (parallelize-forms (seq form) s2c)
        [bs forms] (par-to-bindings ps)]
    {:form (if (seq bs) `(let [~@bs] ~(into (empty form) forms)) form)
     :par (some :par ps) :s2c s2c}))


(defn- parallelize-map [[_ & forms] s2c]
  (when (:trace s2c) (println "parallelize-map" forms))
  (let [ps   (parallelize-forms forms s2c)
        [bs [f & args]] (par-to-bindings ps)
        m1  (if (or (:par (first ps)) (:susp (first ps)))
              `(map deref (map (fn [& xs#] (q/promise (fn [] (apply ~f xs#)))) ~@args ))
              `(map ~f ~@args))]
    {:form (if (seq bs) `(let [~@bs] ~m1) m1) :par (or (some :par ps) (:susp (first ps)))}))


(defn parallelize-let [[_ bs & forms] s2c]
  (when (:trace s2c)  (println "parallelize-let" bs forms))
  (let [[bs1 bs2] (reduce
                   (fn [[bs1 bs2] [s form]]
                     (let [{:keys [form par]} (parallelize form s2c)]
                       (if par
                         (let [ch (gensym "p")]
                           [(concat bs1 [ch `(q/promise (fn [] ~form))])
                            (concat bs2  [s `(deref ~ch)])])
                         [bs1 (concat bs2 [s form])])))
                   [[] []]
                   (partition 2 bs))
        ps  (parallelize-forms forms s2c)]
    {:form `(let [~@(concat bs1 bs2)] ~@(map :form ps)) :par (or (seq bs1) (some :par ps))}))


(defn parallelize
  "Parallelize a form, returning map:
    :form      Parallelized form that can be substituted.
    :bs        Bindings that defines the channel
    :ch        Channel to be dereferenced
    :par       parallel?
    :s2c       Map of user symbols to channel symbols
  To evaluate, one would apply the bindings and then dereference the channel."
  [form & [s2c]]
  (when (:trace s2c)  (println "parallelize " form (type form)))
  (cond
    (nil? s2c)              (parallelize form {})
    (seq? form)
    (if-let [f (as-> (first form) f (when (symbol? f) f))] 
      (cond
        (res= 'let f)    (parallelize-let form s2c)
        (res= 'map f)    (parallelize-map form s2c)
        (function? f)     (parallelize-func form s2c)
        (:macro (meta (resolve f))) (parallelize (macroexpand form) s2c)
        :else            (parallelize-special form s2c))
      (parallelize-coll form s2c))
    (coll? form)             (parallelize-coll form s2c)
    :else                    (parallelize-subst form s2c)))


