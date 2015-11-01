(ns goop.core
  (:use clojure.walk clojure.pprint)
  (:require [ clojure.core.async :as async :refer [<! >! <!! timeout chan alt!! go close!]]
            [clojure.core.async.impl.protocols :as pimpl]
            [clojure.test :refer [function?]]
            [clojure.core.match :refer [match]]
            [clojure.string :as st]
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

(defn build-go-let [forms bs]
  (cond (seq bs)
        `(go (let [~@bs] ~@forms))
        (> (count forms) 1)
        `(go (do ~@forms))
        :else
        `(go ~@forms)))

(defn parallelize [form s2c arghint] ;; => {:form form :s2c s2c :c c :cdef cdef}
  ;(println "Parallelizing" form s2c)
  (cond (and (list? form) (function? (first form)))
        (let [f  (first form)
              ch (gensym arghint)
              ps (map #(parallelize % s2c "ch-") (rest form))
              bs (mapcat :cdef ps)
              args (map :form ps)]
          {:form `(<! ~ch)
           :s2c s2c
           :c ch
           :cdef  [ch (build-go-let [`(~f ~@args)] bs)]})
        (and (list? form) (= 'let (first form)))
        (let [bs (second form)
              forms (nthrest form 2)
              ch (gensym arghint)
              {s2c :s2c bs  :bs}  (reduce (fn [{s2c :s2c  bs :bs} [a v]]
                                            (println "s2c:" s2c "bs:" bs "a:" a "v:" v)
                                            (let [{form :form s2c :s2c cdef :cdef c :c} (parallelize v s2c a)]
                                              (println "parallelized form:" form "s2c:" s2c :cdef cdef  "c:" c)
                                              (if cdef
                                                {:s2c (assoc s2c a c) :bs (concat bs cdef)}
                                                {:s2c s2c :bs (concat bs [a v])})))
                                          {:s2c s2c :bs []}
                                          (partition 2 bs))
              ps (map #(parallelize % s2c "form-") forms)
              bs (apply concat bs (map :cdef ps))
              forms (map :form ps)
              _           (println "After reduction" s2c bs forms)]

          {:form `(<! ~ch)
           :s2c s2c
           :c ch
           :cdef [ch (build-go-let forms bs)]})
        :else
        (do
          ;(println "Default" s2c form)
          {:form (if-let [ch (get s2c form)] `(<! ~ch) form)
           :s2c s2c})))

(defmacro goop [form]
  (let [{form :form cdef :cdef} (parallelize form {} "goop-")
        res (build-go-let [form] cdef)]
    (pprint res)
    res
    ))




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
