(defproject com.andrewmcveigh/refdb "0.7.0-alpha.1"
  :description "File-backed ref-based \"database\""
  :url "http://github.com/andrewmcveigh/refdb"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :release-tasks [["vcs" "assert-committed"]
                  ["clean"]
                  ["test"]
                  ["auto-release" "checkout" "master"]
                  ["auto-release" "merge-no-ff" "develop"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["auto-release" "update-release-notes"]
                  ["auto-release" "update-readme-version"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "v"]
                  ["deploy" "clojars"]
                  ["vcs" "push"]
                  ["auto-release" "checkout" "develop"]
                  ["auto-release" "merge" "master"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]
                  ["auto-release" "checkout-latest-tag"]
                  ["marg"]
                  ["auto-release" "update-marginalia-gh-pages"]]
  :profiles {:dev {:plugins [[com.andrewmcveigh/lein-auto-release "0.1.10"]
                             [lein-marginalia "0.8.0"]]}
             :test {:dependencies [[prismatic/schema "0.3.1"]]}})
