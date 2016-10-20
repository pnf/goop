(ns qaxl.strawman
  (:use clojure.pprint)
  (:require [clojure.string :refer [join]]
            [clojure.test :refer [function?]]
            [clojure.core.match :refer [match]]
            [clojure.core.async :as a :refer [go timeout <! >! <!! >!!]]
            [co.paralleluniverse.pulsar.async :as qa]
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
          (if (seq bs) (->Result (volatile! [:deps bs]))
              (try-get (f coll)))))))

(defn block-apply [f & args] (try-coll args #(apply f %)))
(defn block-map [f coll] (try-coll (try-coll coll #(map f %)) identity))

(def query (memoize (fn [& q] (->Result (volatile! [:query q])))))

(defn reap [b]
  (if-not (instance? Result b) []
          (match [@(.a b)]
                 [[:query _]] [b]
                 [[:deps rs]] (mapcat reap rs)
                 :else [])))

(defn fulfill [r v] (vreset! (.a r) [:value v]) r)

(defn getFirstName [id] (query "select firstName from stuff where id=" id))
(defn getLastName [id] (query "select lastName from stuff where id=" id))
(defn getUserIds [gid] (query "select id from groups where gid=" gid))
(defn getBff [id] (query "select bff from stuff where id=" id))

(defn getBestFriendsNames [gid]
  (block-map #(block-apply getFirstName (getBff %)) (getUserIds gid)))


#_(defn mangleNamesFromGroup [gid]
  (join ", "   (map  #(str ( getFirstName (getBff %)) " " (getLastName %))
                     (getUserIds gid))))

#_(defn mangleNamesFromGroup [gid]
  (let [ids (getUserIds gid)
        lns (getLastNames ids)
        bfs (getBffs ids)
        fns (getFirstNames bfs)]
    (join "," (map #(str %1 " " %2) ids lns))))

#_(defn mangleNamesFromGroup [gid]
  (let [ids (getUserIds gid)
        lns (map getLastName ids)
        bfs (map getBff ids)
        fns (map getFirstName bfs)]
    (join "," (map #(str %1 " " %2) fns lns))))

(defn mangleNamesFromGroup [gid]
  (block-apply join ", "
               (block-map 
                #(block-apply str  (block-apply getFirstName (getBff %)) " "
                              (getLastName %))
                (getUserIds gid))))


(def aanswers {["getIds" 37] [123 321]
               ["getBff" 123] 777
               ["getBff" 321] 888
               ["getName" 777] "Mutt"
               ["getName" 888] "Jeff"
               ["getLastName" 123] "Trump"
               ["getLastName" 321] "Putin"
               })


(defn aquery [s i] (go (println (str s "(" i ") "))
                       (<! (timeout 2000))
                       (get aanswers [s i])))

(defn qaquery [s i] (qa/go (println (str s "(" i ") "))
                       (qa/<! (qa/timeout 2000))
                       (get aanswers [s i])))

(defn mangleNamesFromGroupAsync [gid]
  (a/<!! (a/map vector 
                (map #(go (str (<! (aquery "getName" (<! (aquery "getBff" %))))
                               " " (<! (aquery "getLastName" %))))
                     (a/<!! (aquery "getIds" gid)))))  )

(defn mangleNamesFromGroupAsync2 [gid]
  (a/<!! (a/map vector 
                (map #(go (let [c1 (go (<! (aquery "getName" (<! (aquery "getBff" %)))))
                                c2 (aquery "getLastName" %)]
                            (str (<! c1) " " (<! c2))))
                     (a/<!! (aquery "getIds" gid)))))  )

#_(defn mangleNamesFromGroupAsync3 [gid]
  (qa/<! (qa/map vector 
                 (map #(qa/go (let [c1 (qa/go  (qa/<! (qaquery "getName" (qa/<! (qaquery "getBff" %)))))
                                   c2 (qaquery "getLastName" %)]
                               (str (qa/<! c1) " " (qa/<! c2))))
                     (qa/<! (qaquery "getIds" gid)))))  )

(defn pfs [form]
  ;; Don't do anything unless it looks like a function
  (if (and (seq? form) (function? (first form)))
    (let [[f & args] form                      ;; extract the function and the argument list,
          args (map pfs args)                  ;; recursively transform the arguments
          cs   (repeatedly (count args) gensym)  ;; create a bunch of channels,
          ;;  generate binding block to pair them with the argument forms
          bs   (interleave cs (map (fn [arg] `(a/go ~arg)) args))  ;;  each wrapped in a `go` block,
          ;; and apply the function to the results when they come back.
          args (map (fn [c] `(a/<! ~c)) cs)]
      `(let [~@bs] (~f ~@args)))
    form))
(defmacro parallelize-func-stupid [form] (pfs form))

(comment 
  (def x1 (mangleNamesFromGroup 37)) ;Result[:deps (Result[:query ("select id from groups where gid=" 37)])]
  (-> x1 reap first (fulfill [123 321])) ;; Result[:deps (Result[:value [123 321]])]
  (def x2 (mangleNamesFromGroup 37)) ;Result[:deps (Result[:deps (Result[:deps (Result[:query ("select firstName from stuff where id=" Result[:query ("select bff from stuff where id=" 123)])] Result[:query ("select lastName from stuff where id=" 123)])] Result[:deps (Result[:query ("select firstName from stuff where id=" Result[:query ("select bff from stuff where id=" 321)])] Result[:query ("select lastName from stuff where id=" 321)])])])]
  (def y2 (reap x2)) ; (Result[:query ("select firstName from stuff where id=" Result[:query ("select bff from stuff where id=" 123)])] Result[:query ("select lastName from stuff where id=" 123)] Result[:query ("select firstName from stuff where id=" Result[:query ("select bff from stuff where id=" 321)])] Result[:query ("select lastName from stuff where id=" 321)])
  (doall (map #(do (fulfill %1 %2)) y2 [777 "Trump" 888 "Putin"])) ;; x2 = Result[:waiting (Result[:waiting (Result[:waiting (Result[:waiting (Result[:value 777])] Result[:value "Trump"])] Result[:waiting (Result[:waiting (Result[:value 888])] Result[:value "Putin"])])])]
  (def x3 (mangleNamesFromGroup 37)) ; Result[:waiting (Result[:waiting (Result[:waiting (Result[:query ("select firstName from stuff where id=" 777)])] Result[:waiting (Result[:query ("select firstName from stuff where id=" 888)])])])]
  (def y3 (reap x3)) ;; (Result[:query ("select firstName from stuff where id=" 777)] Result[:query ("select firstName from stuff where id=" 888)])
  (doall (map #(do (fulfill %1 %2)) y3 ["Mutt" "Jeff"]));; x3 = Result[:waiting (Result[:waiting (Result[:waiting (Result[:value "Mutt"])] Result[:waiting (Result[:value "Jeff"])])])]
  (def x4 (mangleNamesFromGroup 37)) ; "Mutt Trump, Jeff Putin"



  (def x1 (getBestFriendsNames 101))  ;; Result[:query ("select id from groups where gid=" [101])]
  (fulfill (-> x1 reap first) [321 123])


  (def x2 (getBestFriendsNames 101))
  ;; Result[:waiting (Result[:waiting (Result[:query ("select bff from stuff where id=" 321)])] Result[:waiting (Result[:query ("select bff from stuff where id=" 123)])])]
  (def y2 (reap x2))
  (map #(fulfill %1 %2) y2 [888 777])
  ;; x2 = Result[:waiting (Result[:waiting (Result[:value 888])] Result[:waiting (Result[:value 777])])]

  (def x3 (getBestFriendsNames 101))
  ;; Result[:waiting (Result[:query ("select firstName from stuff where id=" 888)] Result[:query ("select firstName from stuff where id=" 777)])]
  (def y3 (reap x3))
  (map #(fulfill %1 %2) y3 ["Mutt" "Jeff"])

  (getBestFriendsNames 101)
  ("Mutt" "Jeff")
  )
  
  



