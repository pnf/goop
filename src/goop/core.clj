(ns goop.core
  (:require [ clojure.core.async :as async :refer [<! >! <!! timeout chan alt!! go close!]]
            [clojure.core.async.impl.protocols :as pimpl]
            [clojure.test :refer [function?]])
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

;; Ideally, a goop can be used as-is in normal code and return a value, but can be detected
;; in a goop context.
;; (goop foo [a] (+ a 1))
;; (goop bar [b] (+ b 2))
;; (let [f (foo 3)
;;       g (bar (foo 3))]
;; How could that work?  Well, bar could be a macro?

(comment
  (goop (foo a b ch (bar b) (zap z))) ; =>
  (let [b$1 (goop (bar b))
        z$1 (goop (zap z))]
    (go (foo a b (<! ch) (<! b$1) (<! b$2))))

  )

(defmacro goop [form]
  (if (and (list? form) (symbol? (first form)))
    (let [n    (dec (count form))
          fun  (first form)
          syms (repeatedly n #(gensym "c"))
          chs  (map (fn [c] `(goop ~c)) (rest form))
          bdgs (interleave syms chs)
          vals (map (fn [x] `(<! ~x)) syms)]
      `(go (let [~@bdgs] (fun ~@vals))))
      `(go ~form)
)  )



;; caching
;; value or hash => promise-chan
;; close channel on flush from cache
;;   so fulfillment will need retry logic?  Seems easier than ensuring no active
;;   listeners


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
