(ns qaxl.core
  (:use clojure.walk clojure.pprint qaxl.cache)
  (:require ;[co.paralleluniverse.pulsar.async :as qa]
            [clojure.core.async :as a]
            [co.paralleluniverse.pulsar.core :as q]
            [clojure.test :refer [function?]]
            [co.paralleluniverse.fiber.httpkit.client :as hk]
            [co.paralleluniverse.pulsar.core :refer [fiber]]
            [clojure.core.match :refer [match]]))

(defn- form-to-chan-m [form] `(q/promise (fn [] ~form)))
(defn- await-chan-m [ch] `(deref ~ch))
(defn- await-chan!! [ch] (deref ch))
(defn- deliver-chan-m [ch] `(deliver ~ch))
(defn- fn-chan [args cdef] `(soft-memoize (q/sfn ~args ~cdef)))
(defn- sequence-chan [cs] (q/promise (fn [] (map (fn [c] @c) cs))))

(comment 
  (defmacro gop [form] `(a/pipe (a/go ~form) (a/promise-chan)))
  (defn- form-to-chan-m [form] `(gop ~form))
  (defn- await-chan-m [ch] `(a/<! ~ch))
  (defn- await-chan!! [ch] (a/<!! ch))
  (defn- deliver-chan-m [ch] `(a/>! ~ch))
  (defn- fn-chan [args cdef] `(soft-memoize (fn ~args ~cdef)))
  (defn- sequence-chan [cs] (a/pipe (a/go-loop [acc [] [c & cs] cs]
                                      (let [acc (conj acc (a/<! c))]
                                        (if (seq cs) (recur acc cs) acc)))
                                    (a/promise-chan))))


(declare parallelize)

;; Present as a normal function, except that qfn is a parallelized promise.
(defrecord QaxlFn [qfn]
  clojure.lang.Fn
  clojure.lang.IFn
  ;; Hack, since you can't have variadic implementations.
  (invoke [this x1] (await-chan!! (qfn x1)))
  (invoke [this x1 x2] (await-chan!! (qfn x1 x2)))
  (invoke [this x1 x2 x3] (await-chan!! (qfn x1 x2 x3)))
  (invoke [this x1 x2 x3 x4] (await-chan!! (qfn x1 x2 x3 x4)))
  (invoke [this x1 x2 x3 x4 x5] (await-chan!! (qfn x1 x2 x3 x4 x5)))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defn realize [{:keys [form bs ch par]}]
  (cond (= 2 (count bs)) (await-chan-m  (second bs))
        ch               `(let [~@bs] ~(await-chan-m ch))
        :else            form))

(defn defer   [{:keys [form bs ch par] :as input}]
  (cond (= 2 (count bs)) (second bs)
        ch               `(let [~@bs] ~ch)
        :else            (form-to-chan-m form)))


(defmacro qdefn
  "Like defn, but defines a qaxl function with a parallelized body.
Sadly, doesn't deal with doc strings and metadata."
  [fname args body]
  `(def ~fname (->QaxlFn ~(fn-chan args (defer (parallelize body))))))

(defmacro qfn
  "Like fn, but defines a qaxl function with a parallelized body.
Sadly, doesn't deal with doc strings and metadata."
  [args body]
  `(->QaxlFn ~(fn-chan args (defer (parallelize body)))))

(defmacro qaxl
  "Parallelize and evaluate an expression"
  [form]
  `(await-chan!! ~(defer (parallelize form))))

(defn qaxl-fn
  "If the symbol represents a qaxl function, returns its qfn, otherwise nil."
  [fs]
  (when (or (and (symbol? fs) (some-> fs resolve var-get :qfn))
          (and (list? fs) (= (first fs) 'qfn)))
    `(.qfn ~fs)))

(defn- res= [a b]
  (or (= a b)
      (let [ra (resolve a)
            rb (resolve b)]
        (and ra rb (= (resolve a) (resolve b))))))

;; If there's a channel that will return the value of this symbol, substitute its deref.
(defn- parallelize-subst [form s2c]
  (if-let [ch (and (symbol? form) (get s2c form))]
    {:ch ch :form (await-chan-m ch) :s2c s2c :par true}
    {:form form}))

(defn- parallelize-forms [forms s2c]
  (when (:trace s2c) (println "forms: " forms))
  (let [ps   (map #(parallelize % s2c) forms)
        bs   (mapcat :bs ps)
        fs   (map :form ps)
        par  (some? (some :par ps))]
    [ps bs fs par]))

(defn- std-par [cdef s2c bs]
  (let [ch (gensym "p")]
    {:ch ch :form (await-chan-m ch) :s2c s2c :par true :bs (concat bs [ch cdef])}))

(defn- parallelize-if [[_ &  forms] s2c]
  (when (:trace s2c) (println "if" forms))
  (let [[[q & fs] _ _ par] (parallelize-forms forms s2c)]
    (if par
      (std-par `(if ~(realize q) ~@(map defer fs)) s2c [])
      {:form `(if ~q ~@forms)})))

(defn- parallelize-special [[s & forms] s2c]
  (when (:trace s2c) (println "parallelize-special" s forms))
  (let [[ps _ _ par] (parallelize-forms forms s2c)]
    (if par
      (std-par (form-to-chan-m `(~s ~(map realize ps))) s2c [] )
      {:form `(~s ~@(map realize ps))})))

(defn- parallelize-coll [form s2c]
  (let [[_ bs args par]  (parallelize-forms (seq form) s2c)]
    (if par
      (let [cdef (cond (vector? form) `[~@args] ;; preserve coll type
                       (map? form)    `(hash-map ~@args)
                       (set? form)    `#{~@args}
                       :else          `(list ~@args))]
        (std-par (form-to-chan-m cdef) s2c bs))
      {:form form})))


(defn- parallelize-func [[f & args] s2c]
  (when (:trace s2c) (println "parallelize-func" f args))
  (let [[ps bs args par] (parallelize-forms args s2c)
        qf (qaxl-fn f)]
    (cond qf    (std-par `(~qf ~@args) s2c bs)
          par   (std-par (form-to-chan-m `(~f ~@args)) s2c bs)
          :else {:form `(~f ~@args)})))


(def map-and-deref
  (->QaxlFn
   (fn [g & args] (sequence-chan (apply map g args)))))

(defn- parallelize-map [[_ f & args] s2c]
  (when (:trace s2c) (println "parallelize-map" f args))
  (let [{args :form bs :bs par :par} (parallelize-coll args s2c)
        qfn   (qaxl-fn f)
        mf (if qfn ['map-and-deref qfn] ['map f])]
    (parallelize-func (concat mf args) s2c)))


;; Incorporate a new let binding.
(defn- accrue-bindings [{s2c0 :s2c bs0 :bs} [a v]]
  (let [{:keys [form s2c par bs ch]} (parallelize v s2c0)]
    (if ch
      {:s2c (assoc s2c0 a ch) :bs (concat bs0 bs)}
      {:s2c s2c0              :bs (concat bs0 [a v])})))

(defn- build-let
  "Create a full let form from bindings and contained forms."
  [forms bs]
  (cond (seq bs)             `(let [~@bs] ~@forms)
        (> (count forms) 1)  `(do ~@forms)
        :else                (first forms)))

(defn parallelize-let [[_ bs & forms] s2c]
  (when (:trace s2c)  (println "paralelize-let" bs forms))
  (let [{s2c :s2c bs :bs} (reduce accrue-bindings {:s2c s2c :bs []} (partition 2 bs))
        [ps bsf forms par] (parallelize-forms forms s2c)
        bs (concat bs bsf)]
    (if par
      (std-par (form-to-chan-m (build-let forms bs)) s2c bs)
      {:form (build-let forms bs)})))


(defn parallelize
  "Parallelize a form, returning map:
    :form      Parallelized form that can be substituted.
    :bs        Bindings that defines the channel
    :ch        Channel to be dereferenced
    :par       parallel?
    :s2c       Map of user symbols to channel symbols
  To evaluate, one would apply the bindings and then dereference the channel."
  [form & [s2c]]
  (when (:trace s2c)  (println "paralelize " form (type form)))
  (cond
    (nil? s2c)              (parallelize form {})
    (seq? form)
    (if-let [f (as-> (first form) f (when (symbol? f) f))] 
      (cond
        (res= 'if f)     (parallelize-if form s2c)
        (res= 'let f)    (parallelize-let form s2c)
        (res= 'map f)    (parallelize-map form s2c)
        (function? f)     (parallelize-func form s2c)
        :else            (parallelize-special form s2c))
      (parallelize-coll form s2c))
    (coll? form)             (parallelize-coll form s2c)
    :else                    (parallelize-subst form s2c)))


