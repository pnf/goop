(defproject qaxl "0.1.0-SNAPSHOT"
  :description "Parallelize all the things."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.cache "0.6.5"]
                 [co.paralleluniverse/pulsar "0.7.6"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 ]
  :java-agents [[co.paralleluniverse/quasar-core "0.7.6"]]
  )
