(ns goop.reduce
  (:require [clojure.core.async :as async :refer [<! >! <!! timeout chan promise-chan alt!! go close!]])

  )




(defn assoc-reduce [f coll & [npmax]]
  (let [result (promise-chan)
        npmax  (or npmax 10)
        rc     (chan npmax)]
    (async/go-loop [{:keys [coll front-id results next-id peers np] :as state}
                    {:coll coll  
                     :front-id 0 :next-id 0 :np 0
                     :results (hash-map) :peers (hash-map)}]         
      (println state)
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
              (do ;; reduce next input with existing peer
                (go (>! rc [next-id (f p (first coll)) 1]))
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
