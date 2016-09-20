(ns qaxl.core
  (:use clojure.walk clojure.pprint)
  (:require [co.paralleluniverse.pulsar.async :as qa]
            [clojure.core.async :as async]
            [co.paralleluniverse.pulsar.core :as q]
            [clojure.test :refer [function?]]
            [clojure.core.match :refer [match]]
            [clojure.string :as st]
            [clojure.core.cache :refer (soft-cache-factory)]
            ))


;;(defn- form-to-chan [form] `(a/go ~form))
(defn- form-to-chan [form] `(q/promise (fn [] ~form)))
;;(defn- await-chan [ch] `(a/<! ~ch))
(defn- await-chan [ch] `(deref ~ch))


(def cache (atom (soft-cache-factory {})))

(defn soft-memoize
  "Same as clojure.core/memoize, but uses a soft cache"
  {:static true}
  [f]
  (q/sfn [& args]
    (if-let [e (find @cache (conj args f))]
      (val e)
      (let [ret (apply f args)]
        (swap! cache assoc (conj args f) ret)
        ret))))

(declare parallelize)

;; Present as a normal function, except that qfn is a parallelized promise.
(defrecord QaxlFn [qfn]
  clojure.lang.Fn
  clojure.lang.IFn
  ;; Hack, since you can't have variadic implementations.
  (invoke [this x1] @(qfn x1))
  (invoke [this x1 x2] @(qfn x1 x2))
  (invoke [this x1 x2 x3] @(qfn x1 x2 x3))
  (invoke [this x1 x2 x3 x4] @(qfn x1 x2 x3 x4))
  (invoke [this x1 x2 x3 x4 x5] @(qfn x1 x2 x3 x4 x5))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defmacro qdefn
  "Like defn, but defines a qaxl function with a parallelized body.
Sadly, doesn't deal with doc strings and metadata."
  [fname args body]
  (let [{:keys [form ch bs]} (parallelize body {} "qaxl-")
        cdef (if ch
               `(let [~@bs] ch)
               (form-to-chan body))
        qfn `(soft-memoize (q/sfn ~args ~cdef))]
    `(def ~fname (->QaxlFn ~qfn))))

(defmacro qfn
  "Like fn, but defines a qaxl function with a parallelized body.
Sadly, doesn't deal with doc strings and metadata."
  [args body]
  (let [{:keys [form ch bs]} (parallelize body {} "qaxl-")
        cdef (if ch
               `(let [~@bs] ch)
               (form-to-chan body))
        qfn `(soft-memoize (q/sfn ~args ~cdef))]
    `(->QaxlFn ~qfn)))

(defmacro qaxl
  "Parallelize and evaluate an expression"
  [form]
  (let [{:keys [form ch bs]} (parallelize form {} "qaxl-")]
    (let [res (if ch
                `(let [~@bs] ~(await-chan ch))
                form)]
      ;(pprint res)
      res)))

(defn qaxl-fn
  "If the symbol represents a qaxl function, returns its qfn, otherwise nil."
  [fs]
  (when (or (and (symbol? fs) (some-> fs resolve var-get :qfn))
          (and (list? fs) (= (first fs) 'qfn)))
    `(.qfn ~fs)))

(defn as-qaxl-fn
  "Given a symbol representing a function, make it a qaxl function if it's not already."
  [fs]
  (or (qaxl-fn fs)
      `(qfn [& args#] (apply ~fs args#))))

(defn- res= [a b] (= (resolve a) (resolve b)))

(defmulti parallelize
  "Parallelize a form, returning map:
    :form      Parallelized form that can be substituted.
    :bs        Bindings that defines the channel
    :ch        Channel to be dereferenced
    :par       parallel?
    :s2c       Map of user symbols to channel symbols
  To evaluate, one would apply the bindings and then dereference the channel."
  (fn [form & args]
    (cond
      (nil? args)              :defaults
      (seq? form)
      (if-let [f (first form)]
        (cond (res= 'let f)    :let
              (res= 'map f)    :map
              (or (function? f)
                  (qaxl-fn f)) :func
              :else            :coll)
                               :coll)
      (coll? form)             :coll
      :else                    :subst)))


;; Add default symbol->channel map and channel prefix.
(defmethod parallelize :defaults [form] (parallelize form {} "ch"))

;; If there's a channel that will return the value of this symbol, substitute its deref.
(defmethod parallelize :subst [form s2c chp]
  (if-let [ch (get s2c form)]
    {:ch ch :form (await-chan ch) :s2c s2c :par true}
    {:form form}))

;; Parallelize a collection.
(defmethod parallelize :coll [form s2c chp]
  (let [;;   Recursively parallelize members.
        ps   (map #(parallelize % s2c chp) (seq form))
        ;;   Extract bindings and transformed members
        bs   (mapcat :bs ps)
        args (map :form ps)
        par  (some? (some :par ps))]
    (if-not par
      {:form form}
      (let [ch    (gensym chp)
            ;;    Create parallelized collection of same type as original.
            rhs   (cond (vector? form) `[~@args]
                        (map? form)    `(hash-map ~@args)
                        (set? form)    `#{~@args}
                        :else          `(list ~@args))
            ;;    Bind the new collection to the new channel.
            bs    (concat bs [ch (form-to-chan rhs)])
            form  (await-chan ch)]
        {:form form :par true :bs bs :ch ch}))))

;; Parallelize a function call
(defmethod parallelize :func [[f & args] s2c chp]
  (let [;; Parallelize the arguments.
        {args :form bs :bs par :par} ((get-method parallelize :coll) args s2c chp)
        qfn  (qaxl-fn f)
        ch   (gensym chp)]
    (cond (and par qfn) ;; qaxl call with par args
          {:ch ch :form (await-chan ch)  :par true
           :bs (concat bs [ch `(apply ~qfn ~args)])}
          qfn           ;; qaxl call with ordinary args
          {:ch ch :form (await-chan ch) :par true
           :bs (concat bs [ch `(~qfn ~@args)])}
          par           ;; ordinary call with par args
          {:ch ch :form (await-chan ch)  :par true
           :bs (concat bs [ch (form-to-chan `(apply ~f ~args))])}
          :else         ;; ordinary all around
          {:form `(~f ~@args)})))

(defn map-and-deref [g & args] (map (fn [c] @c) (apply map g args)))

(defmethod parallelize :map [[_ f & args] s2c chp]
  (let [qfn   (qaxl-fn f)
        mf (if qfn ['map-and-deref qfn] ['map f])]
    ((get-method parallelize :func) (concat mf args) s2c chp)))

;; Incorporate a new let binding.
(defn- accrue-bindings [{s2c0 :s2c bs0 :bs} [a v]]
  (let [{:keys [form s2c par bs ch]} (parallelize v s2c0 a)]
    (if ch
      {:s2c (assoc s2c0 a ch) :bs (concat bs0 bs)}
      {:s2c s2c0              :bs (concat bs0 [a v])})))

(defn- build-let
  "Create a full let form from bindings and contained forms."
  [forms bs]
  (cond (seq bs) ;; Actually have bindings.
        `(let [~@bs] ~@forms)
        (> (count forms) 1) ;; No bindings but multiple forms.
        `(do ~@forms)
        :else               ;; Just one form.
        (first forms)))

(defmethod parallelize :let [[_ bs & forms] s2c chp]
  (let [{s2c :s2c bs :bs} (reduce accrue-bindings {:s2c s2c :bs []} (partition 2 bs))
        ps (map #(parallelize % s2c "let-form") forms)
        par (some :par ps) ;; weird if par bindings but not forms...
        bs (apply concat bs (map :bs ps))
        forms (map :form ps)]
    (if par
      (let [ch (gensym chp)
            cdef  (form-to-chan (build-let forms bs))]
        {:par true :ch ch :bs [ch cdef]
         :form (await-chan ch) :s2c s2c})
      {:par false
       :s2c s2c
       :form (build-let forms bs)})))

(def active (atom #{}))
(defn shutdown [] (swap! active #(do (map qa/close! %) #{})))

(defn- batched [reqchan maxlen maxtime]
  (let [outchan (qa/chan (qa/buffer 1000))]
    (qa/go-loop [tout nil acc [] n 0]
      (let [[v c] (qa/alts! (keep identity [tout reqchan]))
            tout  (or tout (qa/timeout maxtime))]
        (if (= c reqchan)
          (if (nil? v)
            (qa/go (when (seq acc) (qa/>! outchan acc))
                   (qa/close! outchan) (qa/close! outchan))
            (let [acc (conj acc v)
                  n   (inc n)]
              (if (>= n maxlen)
                (do (qa/go (qa/>! outchan acc))
                    (recur nil [] 0))
                (recur tout acc n))))
          (do (when (seq acc) (qa/go (qa/>! outchan acc)))
              (recur nil [] 0)))))
    outchan))

(defn fn-batched [fb maxlen maxtime & [buflen]]
  (let [c1  (qa/chan (qa/buffer (or buflen 10000)))
        cb  (batched c1 maxlen maxtime)
        qfn (fn [req] (let [p (q/promise)] (qa/go (qa/>! c1 [req p])) p))]
    (swap! active conj c1 cb)
    (qa/go-loop []
      (when-let [vs-ps @cb]
        (qa/go (doseq [[v p] (map vector (fb (map first vs-ps)) (map second vs-ps))]
                 (deliver p v)))
        (recur)))
    (->QaxlFn qfn)))


(defn adj [x i] (if x (+ x i) i))
(def par-stat (atom {}))
(q/defsfn par-check [id msec]
  (swap! par-stat (fn [h]                     
                    (let [cur (or  (get-in h [id :current]) 0)
                          h   (update-in h [id :current] adj 1)
                          h   (update-in h [id :n] adj 1)
                          h   (update-in h [id :sum] adj cur)]
                      (println (h id))
                      h)))
  (q/sleep msec)
  (swap! par-stat update-in [id :current] adj -1)
  id)


(defn test-quasar-promise
  "Create a huge number of promises and fulfill them all at once"
  [n]
  (let [z (System/nanoTime)
        ps (doall (for [i (range n)] (q/promise #(do (q/sleep 2000) (System/nanoTime)))))
        vs (doall (map deref ps))
        vs (map ( fn [v] (-> v (- z) (* 1.0e-9))) vs)]
    [(apply min vs) (apply max vs)]
    ))



#_(defn test-quasar-fiber
  [n]
  (let [z (System/nanoTime)
        ps (doall (for [i (range n)] (q/spawn-fiber blah)))
        vs (doall (map deref ps))
        vs (map ( fn [v] (-> v (- z) (* 1.0e-9))) vs)]
    [(apply min vs) (apply max vs)]))

(defn test-quasar-fiber-sequence [n]
  (let [t0 (System/nanoTime)
        ts [t0]
        p0 (q/promise)
        pn (loop [n   n
                  p1 p0]
             (let [p2 (q/promise)]
               (q/spawn-fiber #(deliver p2 (inc @p1)))
               (if (pos? n)
                 (recur (dec n) p2)
                 p2)))
        ts (conj ts (System/nanoTime))
        _ (deliver p0 1)
        v @pn
        ts (conj ts (System/nanoTime))
        ts (map ( fn [t] (-> t (- t0) (* 1.0e-9))) ts)]
    [v ts])  )

(defn test-clojure-future-sequence [n]
  (let [t0 (System/nanoTime)
        ts [t0]
        p0 (promise)
        pn (loop [n   n
                  p1 p0]
             (if (zero? n)
               p1
               (recur (dec n) (future (inc @p1)))))
        ts (conj ts (System/nanoTime))
        _ (deliver p0 1)
        v @pn
        ts (conj ts (System/nanoTime))
        ts (map ( fn [t] (-> t (- t0) (* 1.0e-9))) ts)]
    [v ts])  )


(defn test-clojure-async
  [n]
  (let [z (System/nanoTime)
        cs (doall (for [i (range n)] (async/go (do (async/<! (async/timeout 2000)) (System/nanoTime)))))
        vs (doall (map (fn [c] (async/<!! c)) cs))
        vs (map (fn [v] (-> v (- z) (* 1.0e-9))) vs)]
    [(apply min vs) (apply max vs)]
    )
  )



(defn test-clojure-future
  "Create a huge number of promises and fulfill them all at once"
  [n]
  (let [z (System/nanoTime)
        p  (promise)
        f  (fn [i] (let [x (inc i)] (deref p) [(.getId (Thread/currentThread)) (System/nanoTime)]))
        ps (doall (for [i (range n)] (future (f i))))
        _ (do (Thread/sleep 2000) (deliver p 1))
        [ids  vs] (apply mapv vector (doall (map deref ps)))
        ids (set ids)
        vs (map ( fn [v] (-> v (- z) (* 1.0e-9))) vs)]
    [(apply min vs) (apply max vs) (count vs) (count  ids)]
    ))


(comment
  (ns qaxl.core)
  (qdefn foo [x] (+ x 100))
  
  (assert (= (:form (parallelize '(map foo [1 2 3])))
             '(map-and-deref (.qfn foo) [1 2 3])))


  (pprint (parallelize '(let [x (foo 3)] (inc x))))
  #_{:par true,
     :ch g29368,
     :bs
     [g29368
      (co.paralleluniverse.pulsar.core/promise
       (clojure.core/fn
         []
         (clojure.core/let
             [x29365
              ((.qfn foo) 3)
              form-29366
              (co.paralleluniverse.pulsar.core/promise
               (clojure.core/fn [] (clojure.core/list @x29365)))
              form-29367
              (co.paralleluniverse.pulsar.core/promise
               (clojure.core/fn [] (clojure.core/apply inc @form-29366)))]
             @form-29367)))],
     :form @g29368,
     :s2c {x x29365}}

  (pprint (parallelize '(foo 3)))
  #_{:ch g29371, :form @g29371, :par true, :bs (g29371 ((.qfn foo) 3))}

  (pprint (parallelize '(inc 3)))
  #_{:form (inc 3)}

  (pprint (parallelize '(inc (foo 3))))
  #_{:ch g29379,
     :form @g29379,
     :par true,
     :bs
     (g29377
      ((.qfn foo) 3)
      g29378
      (co.paralleluniverse.pulsar.core/promise
       (clojure.core/fn [] (clojure.core/list @g29377)))
      g29379
      (co.paralleluniverse.pulsar.core/promise
       (clojure.core/fn [] (clojure.core/apply inc @g29378))))}
  )


