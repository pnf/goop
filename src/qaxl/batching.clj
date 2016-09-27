(ns qaxl.batching)


(def active (atom #{}))
(defn shutdown [] (swap! active #(do (map a/close! %) #{})))

(defn- batched [reqchan maxlen maxtime]
  (let [outchan (a/chan (a/buffer 1000))]
    (a/go-loop [tout nil acc [] n 0]
      (let [[v c] (a/alts! (keep identity [tout reqchan]))
            tout  (or tout (a/timeout maxtime))]
        (if (= c reqchan)
          (if (nil? v)
            (a/go (when (seq acc) (a/>! outchan acc))
                   (a/close! outchan) (a/close! outchan))
            (let [acc (conj acc v)
                  n   (inc n)]
              (if (>= n maxlen)
                (do (a/go (a/>! outchan acc))
                    (recur nil [] 0))
                (recur tout acc n))))
          (do (when (seq acc) (a/go (a/>! outchan acc)))
              (recur nil [] 0)))))
    outchan))

;; Needs conversion back to go
#_(defn fn-batched [fb maxlen maxtime & [buflen]]
  (let [c1  (a/chan (a/buffer (or buflen 10000)))
        cb  (batched c1 maxlen maxtime)
        qfn (fn [req] (let [p 
                           (q/promise)] (a/go (a/>! c1 [req p])) p))]
    (swap! active conj c1 cb)
    (a/go-loop []
      (when-let [vs-ps @cb]
        (a/go (doseq [[v p] (map vector (fb (map first vs-ps)) (map second vs-ps))]
                 (deliver p v)))
        (recur)))
    (->QaxlFn qfn)))
