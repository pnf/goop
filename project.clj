(defproject qaxl "0.1.0-SNAPSHOT"
  :description "Parallelize all the things."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.cache "0.7.1"]
                 [co.paralleluniverse/pulsar "0.7.9"]
                 [co.paralleluniverse/comsat-httpkit "0.7.0"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/core.match "0.3.0-alpha5"]
                 ]
  :java-agents [[co.paralleluniverse/quasar-core "0.7.9"]]
  :repl-options {:timeout 120000
                 }

  )
