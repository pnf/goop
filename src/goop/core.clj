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
(defmacro go [form] (form-to-chan form))


(defn build-let [forms bs]
  (cond (seq bs)
        `(let [~@bs] ~@forms)
        (> (count forms) 1)
        `(do ~@forms)
        :else
        (first forms)))

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

(declare parallelize parallelize-goop-call parallelize-function-call)

(defrecord GoopFn [gfn]
  clojure.lang.Fn
  clojure.lang.IFn
  (invoke [this x1] (<! (gfn x1)))
  (invoke [this x1 x2] (<! (gfn x1 x2)))
  (invoke [this x1 x2 x3] (<! (gfn x1 x2 x3)))
  (invoke [this x1 x2 x3 x4] (<! (gfn x1 x2 x3 x4)))
  (invoke [this x1 x2 x3 x4 x5] (<! (gfn x1 x2 x3 x4 x5)))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defmacro gdefn [fname args body]
  (let [{:keys [form ch bs]} (parallelize body {} "goop-")
        cdef (if ch
               `(let [~@bs] ch)
               (form-to-chan body))
        gfn `(soft-memoize (q/sfn ~args ~cdef))]
    `(def ~fname (->GoopFn ~gfn))))

(defmacro gfn [args body]
  (let [{:keys [form ch bs]} (parallelize body {} "goop-")
        cdef (if ch
               `(let [~@bs] ch)
               (form-to-chan body))
        gfn `(soft-memoize (q/sfn ~args ~cdef))]
    `(->GoopFn ~gfn)))

(defmacro goop [form]
  (let [{:keys [form ch bs]} (parallelize form {} "goop-")]
    (let [res (if ch
                `(let [~@bs] ~(await-chan ch))
                form)]
      (pprint res)
      res)))

(defn goop-fn [fs]
  (when (or (and (symbol? fs) (some-> fs resolve var-get :gfn))
          (and (list? fs) (= (first fs) 'gfn)))
    `(.gfn ~fs)))

(defn as-goop-fn [fs]
  (or (goop-fn fs)
      `(gfn [& args#] (apply ~fs args#))))


(defn parallelize-coll [form sym->chan ch-prefix]
  (println "parallelize coll" form)
  (let [ps (map #(parallelize % sym->chan ch-prefix) (seq form))
        bs   (mapcat :bs ps)
        args (map :form ps)
        par  (some? (some :par ps))]
    (if par
      (let [ch    (gensym ch-prefix)
            rhs   (cond (vector? form) `[~@args]
                        (map? form)    `(hash-map ~@args)
                        (set? form)    `#{~@args}
                        :else          `(list ~@args))
            bs    (concat bs [ch (form-to-chan rhs)])
            form  (await-chan ch)]
        {:form form :par true :bs bs :ch ch})
      {:form form})))


(defn parallelize-function-call [[f & args] sym->chan ch-prefix]
  (println "parallelize-function-call" f args)
  (let [{args :form bs :bs
         par  :par} (parallelize-coll args sym->chan ch-prefix)
        gfn  (goop-fn f)
        ch   (gensym ch-prefix)]
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
(defn parallelize-map [[_ f & args] sym->chan ch-prefix]
  (println "parallelize-map" args (type args))
  (let [gfn   (goop-fn f)
        mf (if gfn ['map-and-deref gfn] ['map f])]
    (parallelize-function-call (concat mf args) sym->chan ch-prefix)))

(defn parallelize-let [[_ bs & forms] sym->chan ch-prefix]
  (let [;; Parallelize rhs of each binding, accruing sym->chan map.
        {sym->chan :sym->chan bs :bs}  (reduce (fn [{sym->chan0 :sym->chan bs0 :bs} [a v]]
                                                 (println "let loopy" sym->chan0 bs0 a v)
                                                 (let [{:keys [form sym->chan par bs ch]} (parallelize v sym->chan0 a)]
                                                   (println "let loop" a v par bs ch sym->chan0 sym->chan)
                                                   (if ch
                                                     {:sym->chan (assoc sym->chan0 a ch) :bs (concat bs0 bs)}
                                                     {:sym->chan sym->chan0              :bs (concat bs0 [a v])})))
                                               {:sym->chan sym->chan :bs []}
                                               (partition 2 bs))
        ;; Parallelize the forms
        ps (map #(parallelize % sym->chan "form-") forms)
        par (some :par ps)
        ;; Augment bindings with channel definitions for each form
        bs (apply concat bs (map :bs ps))
        forms (map :form ps)]
    (if par
      (let [ch (gensym ch-prefix)
            cdef  (form-to-chan (build-let forms bs))]
        {:par true
         :ch ch
         :bs [ch cdef]
         :form (await-chan ch)
         :sym->chan sym->chan})
      {:par false
       :sym->chan sym->chan
       :form (build-let forms bs)})))

(defn parallelize
  "Returns map
    :form      Parallelized form that can be substituted.
    :bs        Bindings that defines the channel
    :ch        Channel to be dereferenced
    :par       parallel?
    :sym->chan Map of user symbols to channel symbols"
  ([form] (parallelize form {} "g"))
  ([form sym->chan ch-prefix]
   (println "parallelizey" form sym->chan (when (seq? form) (function? (first form))))
   (let [ret (cond (seq? form)
         (if-let [f (first form)]
           (cond (= (resolve 'let) (resolve  f))      (parallelize-let form sym->chan ch-prefix)
                 (= (resolve 'map) (resolve f))      (parallelize-map form sym->chan "map")
                 (or (function? f) (goop-fn f))        (parallelize-function-call form sym->chan ch-prefix)
                 :else           (parallelize-coll form sym->chan ch-prefix))
           '())
         (coll? form)
         (parallelize-coll form sym->chan ch-prefix)
         :else
         (do
           (if-let [ch (get sym->chan form)]
             {:ch ch :form (await-chan ch) :sym->chan sym->chan :par true}
             {:form form})))]
     (println "Returning" ret) ret)
   ))


(comment
  (time (count (qmap #(Math/cos %) (range 500000))))  ;; 1200 ms
  (time (count (pmap #(Math/cos %) (range 500000))))  ;; 2100 ms
  (time (count (map #(Math/cos %) (range 500000)))) ;; 125ms
  )


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



(defn test-quasar-fiber
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
