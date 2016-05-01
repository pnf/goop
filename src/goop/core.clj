(ns goop.core
  (:use clojure.walk clojure.pprint)
  (:require [co.paralleluniverse.pulsar.async :as async :refer [<! >! <!! timeout chan  go close!]]
            [clojure.core.async :as casync ;;:refer [<! >! <!! timeout chan promise-chan alt!! go close!]
             ]
            [co.paralleluniverse.pulsar.core :as q]
            [clojure.core.async.impl.protocols :as pimpl]
            [clojure.test :refer [function?]]
            [clojure.core.match :refer [match]]
            [clojure.string :as st]
            [goop.cache :refer (soft-cache-factory)]
            )
)


(defn promise-chan []
  "Repeatedly delivers first value ever written to it.
Would be nice to detect that nobody was listening anymore."
  (let [c (chan)]
    (go (let [v (<! c)]
          (while (not (pimpl/closed? c))
            (>! c v))
          ))
    c
    ))

(defn build-go-let
  ;; Wrap up a form with some optional bindings as a channel.
  [forms bs]
  (cond (seq bs)
        `(go (let [~@bs] ~@forms))
        (> (count forms) 1)
        `(go (do ~@forms))
        :else
        `(go ~@forms)))

(defn build-q-let
  ;; Wrap up a form with some optional bindings as a channel.
  [forms bs]
  (cond (seq bs)             `(q/promise
                               #(let [~@bs]
                                  ~@forms))
        (> (count forms) 1)  `(q/promise (fn [] (do ~@forms)))
        :else                `(q/promise (fn [] ~@forms))
        ))

(defn goop-call? [form]
  (if (list? form)
    (some-> (first form) (as-> s (and (symbol? s) (resolve s)))
            var-get meta
            :goop)))

(defn parallelize [form s2c arghint] ;; => {:form form :s2c s2c :c c :cdef cdef}
  (cond
    ;; function call
    #_(goop-call? form)
    (and (list? form) (function? (first form)))
    (let [f  (first form)
          ch (gensym arghint)
          ps (map #(parallelize % s2c "ch-") (rest form))
          bs (mapcat :cdef ps)
          args (map :form ps)]
      {:form `(<! ~ch)
       :s2c s2c
       :c ch
       :cdef  [ch (build-go-let [`(~f ~@args)] bs)]})
    ;; let form
    (and (list? form) (= 'let (first form)))
    (let [bs (second form)
          forms (nthrest form 2)
          ch (gensym arghint)
          {s2c :s2c bs  :bs}  (reduce (fn [{s2c :s2c  bs :bs} [a v]]
                                        (let [{form :form s2c :s2c cdef :cdef c :c} (parallelize v s2c a)]
                                          (if cdef
                                            {:s2c (assoc s2c a c) :bs (concat bs cdef)}
                                            {:s2c s2c :bs (concat bs [a v])})))
                                      {:s2c s2c :bs []}
                                      (partition 2 bs))
          ps (map #(parallelize % s2c "form-") forms)
          bs (apply concat bs (map :cdef ps))
          forms (map :form ps)]

      {:form `(<! ~ch)
       :s2c s2c
       :c ch
       :cdef [ch (build-go-let forms bs)]})
    :else
    (do
      {:form (if-let [ch (get s2c form)] `(<! ~ch) form)
       :s2c s2c})))

(defn qparallelize [form s2c arghint] ;; => {:form form :s2c s2c :c c :cdef cdef}
  (cond
    ;; function call
    #_(goop-call? form)
    (and (list? form) (function? (first form)))
    (let [f  (first form)
          ch (gensym arghint)
          ps (map #(parallelize % s2c "ch-") (rest form))
          bs (mapcat :cdef ps)
          args (map :form ps)]
      {:form `(deref ~ch)
       :s2c s2c
       :c ch
       :cdef  [ch (build-q-let [`(~f ~@args)] bs)]})
    ;; let form
    (and (list? form) (= 'let (first form)))
    (let [bs (second form)
          forms (nthrest form 2)
          ch (gensym arghint)
          {s2c :s2c bs  :bs}  (reduce (fn [{s2c :s2c  bs :bs} [a v]]
                                        (let [{form :form s2c :s2c cdef :cdef c :c} (parallelize v s2c a)]
                                          (if cdef
                                            {:s2c (assoc s2c a c) :bs (concat bs cdef)}
                                            {:s2c s2c :bs (concat bs [a v])})))
                                      {:s2c s2c :bs []}
                                      (partition 2 bs))
          ps (map #(parallelize % s2c "form-") forms)
          bs (apply concat bs (map :cdef ps))
          forms (map :form ps)]

      {:form `(deref ~ch)
       :s2c s2c
       :c ch
       :cdef [ch (build-q-let forms bs)]})
    :else
    (do
      {:form (if-let [ch (get s2c form)] `(deref ~ch) form)
       :s2c s2c})))


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


(defmacro qoop [form]
  (let [{form :form cdef :cdef} (qparallelize form {} "qoop-")
        res (build-q-let [form] cdef)]
    (pprint res)
    res
    ))

(defmacro goop [form]
  (let [{form :form cdef :cdef} (parallelize form {} "goop-")
        res (build-go-let [form] cdef)]
    (pprint res)
    res
    ))

(defmacro gn [args & forms]
  `(with-meta
     (fn ~args ~@forms)
     {:goop true}))

(defmacro defgoop [fname args & forms]
  `(def ~fname (with-meta
                 (fn ~args ~@forms)
                 {:goop true})))



(def cache (soft-cache-factory {}))

(comment
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


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
