(ns qaxl.strawman
  (:use clojure.pprint)
  (:require [clojure.string :refer [join]]
            [clojure.core.match :refer [match]]
            [clojure.core.async :as a :refer [go timeout <! >! <!! >!!]]
            ))

(deftype Result [a])
(defmethod print-method Result [o, w] (.write w (str "Result" @(.a o))))


(defn try-get [r]
  (if-not (instance? Result r) r
          (match [@(.a r)]
                 [[:value v]] v
                 :else r)))

(defn try-coll [coll f]
  (let [coll (try-get coll)]
    (if (instance? Result coll) coll
        (let [coll (map try-get coll)
              bs   (filter #(instance? Result %) coll)]
          (if (seq bs) (->Result (atom [:deps bs]))
              (try-get (f coll)))))))

(defn block-apply [f & args] (try-coll args #(apply f %)))
(defn block-map [f coll] (try-coll (try-coll coll #(map f %)) identity))

(def query (memoize (fn [& q] (->Result (atom [:query q])))))

(defn reap [b]
  (if-not (instance? Result b) []
          (match [@(.a b)]
                 [[:query _]] [b]
                 [[:deps rs]] (mapcat reap rs)
                 :else [])))

(defn fulfill [r v] (reset! (.a r) [:value v]) r)

(defn getFirstName [id] (query "select firstName from stuff where id=" id))
(defn getLastName [id] (query "select lastName from stuff where id=" id))
(defn getUserIds [gid] (query "select id from groups where gid=" gid))
(defn getBff [id] (query "select bff from stuff where id=" id))

(defn formatNamesFromGroup [gid]
  (block-apply join ", "
               (block-map 
                #(block-apply str (getFirstName %) " " (getLastName %))
                (getUserIds gid))))


(defn getBestFriendsNames [gid]
  (block-map #(block-apply getFirstName (getBff %)) (getUserIds gid)))


(comment 
  (def x1 (formatNamesFromGroup 37))
  (-> x1 reap first (fulfill [237 401]))
  (def x2 (formatNamesFromGroup 37))
  (def y2 (reap x2))
  (doall (map #(do (fulfill %1 %2)) y2 ["Don" "Trump" "Al" "Putin"]))
  (def x3 (formatNamesFromGroup 37))

  (def x1 (getBestFriendsNames 101))  ;; Result[:query ("select id from groups where gid=" [101])]
  (fulfill (-> x1 reap first) [321 123])

  (def x2 (getBestFriendsNames 101))
  ;; Result[:deps (Result[:deps (Result[:query ("select bff from stuff where id=" 321)])] Result[:deps (Result[:query ("select bff from stuff where id=" 123)])])]
  (def y2 (reap x2))
  (map #(fulfill %1 %2) y2 [888 777])
  ;; x2 = Result[:deps (Result[:deps (Result[:value 888])] Result[:deps (Result[:value 777])])]

  (def x3 (getBestFriendsNames 101))
  ;; Result[:deps (Result[:query ("select firstName from stuff where id=" 888)] Result[:query ("select firstName from stuff where id=" 777)])]
  (def y3 (reap x3))
  (map #(fulfill %1 %2) y3 ["Mutt" "Jeff"])

  (getBestFriendsNames 101)
  ("Mutt" "Jeff")
  )
  
  



