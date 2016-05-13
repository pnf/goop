(ns goop.reduce
  (:require [clojure.core.async :as async :refer [<! >! <!! timeout chan promise-chan alt! alts! go pipe]]
            [clojure.core.async.lab :refer [spool]]
            ))


(defn assoc-reduce [f coll & [npmax]]
  (let [result (promise-chan)
        npmax  (or npmax 10)
        rc     (chan npmax)]
    (async/go-loop [{:keys [coll front-id results next-id peers np] :as state}
                    {:coll coll  
                     :front-id 0 :next-id 0 :np 0
                     :results (hash-map) :peers (hash-map)}]         
      (prn state)
      (if-let [[a l] (results front-id)] ;; result ready?
        (if-let [p (peers l)]
          (do (go (>! rc [next-id (f p a) (inc l)])) ;; reduce result with existing peer
              (recur (assoc state
                            :front-id (inc front-id) :next-id (inc next-id)
                            :results (dissoc results front-id) :peers (dissoc peers l)
                            :np (inc np))))
          (recur (assoc state ;; set result as new peer
                        :peers (assoc peers l a) 
                        :front-id (inc front-id)
                        :results (dissoc results front-id))))
        (if (and (pos? np) ;; no results ready; reductions running?
                 (or (>= np npmax) (not (seq coll)))) ;; can't add any more reductions
          (let [[i a l] (<! rc)] ;; wait for a result and stash it away
            (recur (assoc state :results (assoc results i [a l]) :np (dec np))))
          (if (seq coll) ;; not waiting; do we have inputs?
            (if-let [p (peers 0)]
              (let [c1 (first coll)] ;; reduce next input with existing peer
                (go (try (>! rc [next-id (f p c1) 1]) (catch Exception e (prn "Exc" p c1 next-id e)))  )
                (recur (assoc state :coll (rest coll) :next-id (inc next-id) :peers (dissoc peers 0) :np (inc np))))
              (recur (assoc state :coll (rest coll) :peers (assoc peers 0 (first coll))))) ;; set next input as new peer
            ;; No results, inputs or pending reductions, so reduce the peers if we have any...
            (if (pos? (count (dissoc  peers 0)))
              (recur (assoc state :coll (reverse (map peers (sort (keys peers))))
                            :front-id 0 :next-id 0
                            :results (hash-map) :peers (hash-map)
                            :np 0))
              (>! result (peers 0))))))) ;; otherwise return final result
    result))


(defn delay-spool [as t]
  (let [c (chan)]
    (async/go-loop [[a & as] as]
      (if a
        (do a
            (>! c a)
            (<! (timeout (rand-int t)))
            (recur as)
            )
        (async/close! c)))
    c))




(defn assoc-reduce2 [f c-in & {:keys [np-max debug] :or {np-max 10 debug false}}]
  (let [c-result (promise-chan)
        c-redn    (chan np-max)]
    (let [with-reduction (fn [a l {:keys [front-id next-id peers np] :as state}]
                           (if-let [p (peers l)]
                             (do (go  (>! c-redn [next-id (<! (f p a)) (inc l)]))
                                 (assoc state :next-id (inc next-id) :peers (dissoc peers l) :np (inc np)))
                             (assoc state :peers (assoc peers l a))))
          default-state   {:c-in c-in :front-id 0 :next-id 0 :np 0 :results (hash-map) :peers (hash-map)}]
      (async/go-loop [{:keys [c-in front-id results next-id peers np] :as state} default-state]
        (when debug  (prn (dissoc state :c-in)))
        (if-let [[a l] (results front-id)]  ;; If head result read, reduce with peer
          (recur  (with-reduction a l (assoc state :results (dissoc results front-id) :front-id (inc front-id))))
          (if-let [cs (seq (filter identity (list (if (pos? np) c-redn) c-in)))]
            (let [[v c] (alts! cs)] ;; Listen for reductions and/or inputs
              (condp = c
                c-redn (let [[i a l] v] ;; got a reduction
                         (recur (assoc state :results (assoc results i [a l]) :np (dec np))))
                c-in   (if v ;; got an input or and end-of-input
                         (recur (with-reduction v 0 state))
                         (recur (assoc state :c-in nil)))))
            (if (pos? (count (dissoc peers 0)))
              (recur (assoc default-state :c-in (spool (reverse (map peers (sort (keys peers)))))))
              (>! c-result (peers 0))))))) ;; otherwise return final result
    c-result))


(defn pretty-state [{:keys [peers np]}]
  (let [peers (->> (seq peers)
                   (sort-by first)
                   (filter second)
                   (map (fn [[l ps]]
                          (let [psd  (map deref ps)]
                            [l (take-while identity (take 2 psd)) (count psd) (count (filter not psd))]))))]
    [np peers]))


(defn launch-reductions [c-redn f l ps]
  (let [pairs (take-while (fn [[a b]] (and a b))
                          (partition 2 (map deref ps)))]
    (map (fn [[a b]]
           (let [v (volatile! nil)]
             (go (>! c-redn [(inc l) (<! (f a b)) v]))
             v)) pairs)))

(defn wrapv [c] (pipe c (chan 1 (map (fn [x] [0 x nil])))))


(defn assoc-reduce3 [f c-in & {:keys [np-max debug] :or {np-max 10 debug false}}]
  (let [c-result (promise-chan)
        c-redn    (chan np-max)]
    (async/go-loop [{:keys [c-in peers np] :as state} {:c-in (wrapv c-in) :peers {} :np 0}]
      (if debug (prn (pretty-state state)))
      (if-let [cs (seq (filter identity (list (if (pos? np) c-redn) c-in)))]
        (let [[[l res v]  c]  (alts! cs)]
          (if-not l
            (recur (assoc state :c-in nil))
            (let [ps        (if v
                              (do (vreset! v res) (peers l))
                              (concat (peers 0) [(volatile! res)]))
                  vs        (launch-reductions c-redn f l ps)
                  ps        (drop (* (count vs) 2) ps )
                  np        (cond-> (+ np (count vs)) (pos? l) dec)
                  l2        (inc l)
                  ps2       (concat (peers l2) vs)]
              (recur (assoc state :np np :peers (assoc peers l ps l2 ps2))))))
        (let [reds (->> (seq peers)
                        (sort-by first)
                        (map second)
                        (map first)
                        (filter identity)
                        (map deref)
                        reverse
                        )]
          (if debug (prn "Reducing reductions" reds))
          (if (<= (count reds) 1)
            (>! c-result (first reds))
            (recur {:c-in (wrapv (spool reds)) :peers {} :np 0})))))
    c-result))
