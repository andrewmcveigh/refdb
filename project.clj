(defproject com.andrewmcveigh/refdb "0.5.2-SNAPSHOT"
  :description "File-backed ref-based \"database\""
  :url "http://github.com/andrewmcveigh/refdb"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :plugins [[com.andrewmcveigh/lein-auto-release "0.1.5"]]
  :release-tasks [["vcs" "assert-committed"]
                  ["auto-release" "checkout" "master"]
                  ["auto-release" "merge-no-ff" "develop"]
                  ["change" "version"
                   "leiningen.release/bump-version" "release"]
                  ["auto-release" "update-release-notes"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v"]
                  ["deploy" "clojars"]
                  ["vcs" "push"]
                  ["auto-release" "checkout" "develop"]
                  ["auto-release" "merge" "master"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]
  :profiles {:dev {:plugins [[lein-marginalia "0.7.1"]]}
             :test {:dependencies [[prismatic/schema "0.3.1"]]}}
  :aliases {"deploy" ["do" ["clean"] ["test"] ["deploy" "clojars"]]})
