(ns qaxl.core
  (:use clojure.walk clojure.pprint qaxl.cache)
  (:require [co.paralleluniverse.pulsar.async :as qa]
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
(defn- parallelize-subst [s s2p]
  (if (symbol? s)
    (if (contains? s2p s)
      {:form `(deref ~(get s2p s)) :s2p s2p :par true}
      {:form s :par (some-> s resolve var-get q/suspendable?) :s2p s2p})
    {:form s :s2p s2p}))


(defn- parallelize-forms [forms s2p] (map #(parallelize % s2p) forms))
(defn- par-to-bindings [forms s2p] ;; => [bs args]
  (let [ps  (parallelize-forms forms s2p)
        [bs args] (reduce (fn [[bs args] p]
                            (if (:par p)
                              (let [ch (gensym "p")]
                                [(concat bs [ch `(q/promise (fn [] ~(:form p)))])
                                 (conj args `(deref ~ch))])
                              [bs (conj args (:form p))]))
                          [[] []]
                          ps)]
    [ps bs (seq args)]))


(defn- parallelize-func [forms s2p]
  (when (:trace s2p) (println "parallelize-func" forms))
  (let [[ps bs forms] (par-to-bindings forms s2p)
        par        (some :par ps)]
    {:form (if (seq bs) `(let [~@bs] (~@forms)) `(~@forms)) :par par :s2p s2p}))

;; Possibly lazy
(defn- parallelize-special [[s & forms] s2p]
  (when (:trace s2p) (println "parallelize-special" s forms))
  (let [ps (parallelize-forms forms s2p)]
    {:form `(~s ~@(map :form ps)) :par (some :par ps) :s2p s2p}))

(defn- parallelize-coll [form s2p]
  (let [[ps bs forms] (par-to-bindings (seq form) s2p)]
    {:form (if (seq bs) `(let [~@bs] ~(into (empty form) forms)) form)
     :par (some :par ps) :s2p s2p}))


(defn- parallelize-map [[_ & forms] s2p]
  (when (:trace s2p) (println "parallelize-map" forms))
  (let [[ps bs [f & args]] (par-to-bindings forms s2p)
        m1  (if-not (:par (first ps))
              `(map ~f ~@args)
              `(map deref
                    (doall (map (fn [& xs#]
                                  (q/promise (fn [] (apply ~f xs#))))
                                ~@args ))))]
    {:form (if (seq bs) `(let [~@bs] ~m1) m1) :par (some :par ps)}))


(defn parallelize-let [[_ bs & forms] s2p]
  (when (:trace s2p)  (println "parallelize-let" bs forms))
  (let [[bs1 bs2] (reduce
                   (fn [[bs1 bs2 s2p] [s form]]
                     (let [{:keys [form par]} (parallelize form s2p)]
                       (if par
                         (let [ch (gensym "p")]
                           [(concat bs1 [ch `(q/promise (fn [] ~form))])
                            (concat bs2  [s `(deref ~ch)])
                            (assoc s2p s ch)])
                         [bs1 (concat bs2 [s form]) s2p])))
                   [[] [] s2p]
                   (partition 2 bs))
        ps  (parallelize-forms forms s2p)]
    {:form `(let [~@(concat bs1 bs2)] ~@(map :form ps)) :par (or (seq bs1) (some :par ps))}))


(defn parallelize
  "Parallelize a form, returning map:
    :form      Parallelized form that can be substituted.
    :par       parallel?
    :s2p       Map of user symbols to channel symbols
  To evaluate, one would apply the bindings and then dereference the channel."
  [form & [s2p]]
  (when (:trace s2p)  (println "parallelize " form (type form)))
  (cond
    (nil? s2p)              (parallelize form {})
    (seq? form)
    (if-let [f (as-> (first form) f (when (symbol? f) f))] 
      (cond
        (res= 'let f)    (parallelize-let form s2p)
        (res= 'map f)    (parallelize-map form s2p)
        (function? f)     (parallelize-func form s2p)
        (:macro (meta (resolve f))) (parallelize (macroexpand form) s2p)
        :else            (parallelize-special form s2p))
      (parallelize-coll form s2p))
    (coll? form)             (parallelize-coll form s2p)
    :else                    (parallelize-subst form s2p)))

(defmacro parallelize-func-stupid [form]
  ;; Extract the function and its arguments,
  (let [[f & args] form
        ;; make a bunch of channels
        cs   (repeatedly (count args) gensym)
        ;; create binding form arguments to match them with the
        ;; arguments, now launched in their own go blocks.
        bs   (interleave cs (map (fn [arg] `(a/go ~arg)) args))
        ;; Now we just pull out the results:
        args (map (fn [c] `(a/<! ~c)) cs)]
    `(let [~@bs] (~f ~@args))))

(defmacro parallelize-func-stupid2 [form]
  (let [[f & args] form
        ps   (repeatedly (count args) gensym)
        bs   (interleave ps (map (fn [arg] `(q/promise (fn [] ~arg))) args))
        args (map (fn [p] `(deref ~p)) ps)]
    `(let [~@bs] (~f ~@args))))
