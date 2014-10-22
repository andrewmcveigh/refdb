(defproject com.andrewmcveigh/refdb "0.4.1"
  :description "File-backed ref-based \"database\""
  :url "http://github.com/andrewmcveigh/refdb"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [riddley "0.1.7"]]
  :profiles {:dev {:plugins [[lein-marginalia "0.7.1"]]}})
