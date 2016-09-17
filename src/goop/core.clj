(ns goop.core
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
(defn form-to-chan [form] `(q/promise (fn [] ~form)))
;;(defn- await-chan [ch] `(a/<! ~ch))
(defn await-chan [ch] `(deref ~ch))
(defmacro <! [ch] (await-chan ch))

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

;; Present as a normal function, except that gfn is a parallelized promise.
(defrecord GoopFn [gfn]
  clojure.lang.Fn
  clojure.lang.IFn
  ;; Hack, since you can't have variadic implementations.
  (invoke [this x1] (<! (gfn x1)))
  (invoke [this x1 x2] (<! (gfn x1 x2)))
  (invoke [this x1 x2 x3] (<! (gfn x1 x2 x3)))
  (invoke [this x1 x2 x3 x4] (<! (gfn x1 x2 x3 x4)))
  (invoke [this x1 x2 x3 x4 x5] (<! (gfn x1 x2 x3 x4 x5)))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defmacro gdefn
  "Like defn, but defines a goop function with a parallelized body.
Sadly, doesn't deal with doc strings and metadata."
  [fname args body]
  (let [{:keys [form ch bs]} (parallelize body {} "goop-")
        cdef (if ch
               `(let [~@bs] ch)
               (form-to-chan body))
        gfn `(soft-memoize (q/sfn ~args ~cdef))]
    `(def ~fname (->GoopFn ~gfn))))

(defmacro gfn
  "Like fn, but defines a goop function with a parallelized body.
Sadly, doesn't deal with doc strings and metadata."
  [args body]
  (let [{:keys [form ch bs]} (parallelize body {} "goop-")
        cdef (if ch
               `(let [~@bs] ch)
               (form-to-chan body))
        gfn `(soft-memoize (q/sfn ~args ~cdef))]
    `(->GoopFn ~gfn)))

(defmacro goop
  "Parallelize and evaluate an expression"
  [form]
  (let [{:keys [form ch bs]} (parallelize form {} "goop-")]
    (let [res (if ch
                `(let [~@bs] ~(await-chan ch))
                form)]
      ;(pprint res)
      res)))

(defn goop-fn
  "If the symbol represents a goop function, returns its gfn, otherwise nil."
  [fs]
  (when (or (and (symbol? fs) (some-> fs resolve var-get :gfn))
          (and (list? fs) (= (first fs) 'gfn)))
    `(.gfn ~fs)))

(defn as-goop-fn
  "Given a symbol representing a function, make it a goop function if it's not already."
  [fs]
  (or (goop-fn fs)
      `(gfn [& args#] (apply ~fs args#))))

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
                  (goop-fn f)) :func
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
        gfn  (goop-fn f)
        ch   (gensym chp)]
    (cond (and par gfn) ;; goop call with par args
          {:ch ch :form (await-chan ch)  :par true
           :bs (concat bs [ch `(apply ~gfn ~args)])}
          gfn           ;; goop call with ordinary args
          {:ch ch :form (await-chan ch) :par true
           :bs (concat bs [ch `(~gfn ~@args)])}
          par           ;; ordinary call with par args
          {:ch ch :form (await-chan ch)  :par true
           :bs (concat bs [ch (form-to-chan `(apply ~f ~args))])}
          :else         ;; ordinary all around
          {:form `(~f ~@args)})))

(defn map-and-deref [g & args] (map (fn [c] (<! c)) (apply map g args)))
(defmethod parallelize :map [[_ f & args] s2c chp]
  (let [gfn   (goop-fn f)
        mf (if gfn ['map-and-deref gfn] ['map f])]
    (parallelize-function-call (concat mf args) s2c chp)))

;; Incorporate a new let binding.
(defn- accrue-bindings [{s2c0 :s2c bs0 :bs} [a v]]
  (let [{:keys [form s2c par bs ch]} (parallelize v s2c0 a)]
    (if ch
      ;; Record the channel to deref to get the value of the symbol, and the binding to
      ;; define the channel.
      {:s2c (assoc s2c0 a ch) :bs (concat bs0 bs)}
      ;; Non-parallel term.  Just add the plain binding.
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
        ;; If there were parallel bindings, but not forms, then someone is launching channels
        ;; unnecessarily, but no matter.
        par (some :par ps)
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
  (ns goop.core)
  (gdefn foo [x] (+ x 100))
  
  (assert (= (:form (parallelize '(map foo [1 2 3])))
             '(map-and-deref (.gfn foo) [1 2 3])))


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
              ((.gfn foo) 3)
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
  #_{:ch g29371, :form @g29371, :par true, :bs (g29371 ((.gfn foo) 3))}

  (pprint (parallelize '(inc 3)))
  #_{:form (inc 3)}

  (pprint (parallelize '(inc (foo 3))))
  #_{:ch g29379,
     :form @g29379,
     :par true,
     :bs
     (g29377
      ((.gfn foo) 3)
      g29378
      (co.paralleluniverse.pulsar.core/promise
       (clojure.core/fn [] (clojure.core/list @g29377)))
      g29379
      (co.paralleluniverse.pulsar.core/promise
       (clojure.core/fn [] (clojure.core/apply inc @g29378))))}
  )


