(ns goop.core
  (:use clojure.walk clojure.pprint)
  (:require [co.paralleluniverse.pulsar.async :as a]
            [clojure.core.async]
            [co.paralleluniverse.pulsar.core :as q]
            [clojure.test :refer [function?]]
            [clojure.core.match :refer [match]]
            [clojure.string :as st]
            [goop.cache :refer (soft-cache-factory)]
            ))

(defn- form-to-chan [form] `(a/go ~form))
;;(defn launch-form [form] `(q/promise (fn [] ~form)))
(defn- await-chan [ch] `(a/<! ~ch))
(defmacro <! [ch] (await-chan ch))
(defmacro go [form] (form-to-chan form))
;;(defn await-chan [ch] `(deref ~ch))


(defn build-let [forms bs]
  (cond (seq bs)
        `(let [~@bs] ~@forms)
        (> (count forms) 1)
        `(do ~@forms)
        :else
        (first forms))
  )


(declare parallelize parallelize-goop-call parallelize-function-call)

(defrecord GoopFn [gfn]
  clojure.lang.IFn
  (invoke [this x1] (<! (gfn x1)))
  (invoke [this x1 x2] (<! (gfn x1 x2)))
  (invoke [this x1 x2 x3] (<! (gfn x1 x2 x3)))
  (invoke [this x1 x2 x3 x4] (<! (gfn x1 x2 x3 x4)))
  (invoke [this x1 x2 x3 x4 x5] (<! (gfn x1 x2 x3 x4 x5)))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defmacro defgoop [fname args form]
  (let [{form :form cdef :cdef} (parallelize form {} "goop-")
        cdef (or cdef (form-to-chan form))
        gfn `(fn ~args ~cdef)]
    `(def ~fname (->GoopFn ~gfn))))

#_(defmacro defgoop [fname args form]
  (let [gf (gensym (str fname "-goop-") )
        gfs (name gf)
        {form :form cdef :cdef} (parallelize form {} "goop-")
        cdef (or cdef `(go ~form))]
    `(do
       (defn ~gf ~args ~cdef)
       (def ~fname (with-meta
                     (fn ~args (await-chan `(~gf ~@args)))
                     {:goop ~gfs})))))

(defmacro goop [form]
  (let [{form :form cdef :cdef bs :ch-bind} (parallelize form {} "goop-")]
    (if cdef
      `(let [~@bs] ~(await-chan cdef))
      form)))

(defn goop-fn [fs]
  (let [f (-> fs (as-> s (and symbol? s) (resolve s)) var-get)]
    (when (instance? GoopFn f) (:gfn f))))

#_(defn goop-fn [f]
  (some-> f (as-> s (and (symbol? s) (resolve s)))
          var-get
          meta
          :goop
          symbol
          resolve))


(defn parallelize-goop-call [gf args sym->chan ch-prefix]
  (let [ps   (map #(parallelize % sym->chan "ch-") args)
        bs   (mapcat :ch-bind ps)
        args (map :form ps)
        ch   (gensym ch-prefix)
        cdef (build-let [`(~gf ~@args)] bs)]
    (println "parallelize-goop-call" gf args ps)
    {:sym->chan sym->chan
     :form (await-chan ch)
     :par true
     :c ch
     :cdef cdef
     :ch-bind [ch cdef]}))


(defn parallelize-function-call [f args sym->chan ch-prefix]
  (println "parallelize-function-call" f args (type args))
  (let [
        ps   (map #(parallelize % sym->chan "ch-") args)
        bs   (mapcat :ch-bind ps)
        args (map :form ps)
        par  (some :par ps)
        _ (println ps)        ]
    (merge {:sym->chan sym->chan
            :par par
            :ch-bind []}
           (if par ;; Invoking a regular function with par args
             (let [ch   (gensym ch-prefix)
                   cdef `(go ~(build-let [`(~f ~@args)] bs))]
               {:form (await-chan ch)
                :par true
                :c ch
                :cdef cdef
                :ch-bind [ch cdef]})
             {:form (build-let [`(~f ~@args)] bs)}))))

(defn map-and-deref [g & args] (map (fn [c] (<! c)) (apply map g args)))

(defn parallelize-map [f args sym->chan ch-prefix]
  (println "parallelize-map" args (type args))
  (if-let [gf  (goop-fn f)]
    (parallelize-function-call 'map-and-deref (concat [gf] args) sym->chan ch-prefix)
    (parallelize-function-call 'map (concat [f] args) sym->chan ch-prefix)))


(defn parallelize-fn [[args form] sym->chan ch-prefix]
  (let [p (parallelize form sym->chan ch-prefix)
        cdef (:cdef p)]
    (if cdef
      {:form `(->GoopFn (fn ~args ~cdef))
       :par true
       :sym->chan sym->chan
       :ch-bind []}
      {:form `(fn ~args ~form)
       :par false})))


(defn parallelize-let [form sym->chan ch-prefix]
(let [bs (second form)
          forms (nthrest form 2)
          ;; Parallelize rhs of each binding, accruing sym->chan map.
          {sym->chan :sym->chan bs :bs}  (reduce (fn [{:keys [sym->chan bs]} [a v]]
                                                   (let [{:keys [form sym->chan cdef ch-bind c]} (parallelize v sym->chan a)]
                                                      (if cdef
                                                        {:sym->chan (assoc sym->chan a c) :bs (concat bs ch-bind)}
                                                        {:sym->chan sym->chan             :bs (concat bs [a v])})))
                                                  {:sym->chan sym->chan :bs []}
                                                  (partition 2 bs))
          ;; Parallelize the forms
          ps (map #(parallelize % sym->chan "form-") forms)
          par (some :par ps)
          ;; Augment bindings with channel definitions for each form
          bs (apply concat bs (map :ch-bind ps))
          forms (map :form ps)]
      (merge {:sym->chan sym->chan
              :par par}
             (if par
               (let [ch (gensym ch-prefix)
                     cdef  `(go ~(build-let forms bs))]
                 {:c ch
                  :cdef cdef
                  :ch-bind [ch cdef]
                  :form (await-chan ch)})
               {:form (build-let forms bs)})))  )

(defn parallelize
  "Returns map
    :form      Parallelized form that can be substituted.
    :c         Symbol of channel that returns the contents of the form
    :cdef      Binding that defines the channel
    :sym->chan Map of user symbols to channel symbols"
  [form sym->chan ch-prefix]
  (println "parallelize" form)
  (cond
    (and (list? form) (ifn? (first form)))
    (let [f   (first form)
          gf (goop-fn f)]
      (if gf
        (parallelize-goop-call gf (rest form) sym->chan ch-prefix)
        (parallelize-function-call f (rest form) sym->chan ch-prefix)))
    (and (list? form) (= 'let (first form)))
    (parallelize-let form sym->chan ch-prefix)
    (and (vector? form) (seq form))
    (parallelize-function-call 'vector form sym->chan ch-prefix)
    (list? form)
    (parallelize-function-call 'seq form sym->chan ch-prefix)
    :else
    {:form (if-let [ch (get sym->chan form)] (await-chan ch) form)
       :sym->chan sym->chan}))


(comment
  (time (count (qmap #(Math/cos %) (range 500000))))  ;; 1200 ms
  (time (count (pmap #(Math/cos %) (range 500000))))  ;; 2100 ms
  (time (count (map #(Math/cos %) (range 500000)))) ;; 125ms
  )



(defn qmap
  ([f c]
   (map deref (map (fn [x] (q/promise (fn [] (f x)))) c)))
  ([f c c2]
   (map deref (map (fn [x] (q/promise (fn [] (f x)))) c c2)))
  ([f c1 c2 & cs]
   (map deref (apply map (fn [x1 x2 & xs] (q/promise (fn [] (apply f x1 x2 xs)))) c1 c2 cs))) )



(def cache (soft-cache-factory {}))



(comment

  "Currently parallelizing all arguments to all functions.
Can we parallelize only forms that contain something goopy?

"

  "TODO


"
  


  "How should one handle stream operations?
Return channel containing results?
Return channel returning channels?
"

  
  (def get-promise [f args]
    (if-let [c (get cache [f args])]
      c
      (let [p (promise-chan)]
        (assoc cache [f args] p)
        (go (>! p (apply f args)))))))

(def ^:dynamic *goopy* false)





(comment
 

  "
With goopy bound true, a goop returns a promise channel;
Otherwise returns value.




")




;; (comment

;;     (symbol? form)
;;     (or (get ch-bindings form) form)

;;     (= hd 'let) ;; parallelize bindings
;;             (let [[bdgs & body] args
;;                   [syms vals] (apply mapv vector (partition 2 bdgs))
;;                   ch-syms (repeatedly (count syms) #(gensym "c"))
;;                   ch-bindings (apply assoc ch-bindings (interleave syms ch-syms))
;;                   goops   (map (fn [a] `(goop ~a ch-bindings)) vals)
;;                   bdgs  (vec  (interleave ch-syms goops))]
;;               `(let ~bdgs (goop ~body)))

;;   )



;; caching
;; value or hash => promise-chan
;; close channel on flush from cache
;;   so fulfillment will need retry logic?  Seems easier than ensuring no active
;;   listeners



