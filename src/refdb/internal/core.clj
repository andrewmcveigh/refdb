(ns refdb.internal.core
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]))

(defn load-form [file]
  (when (.exists file)
    (with-open [reader (java.io.PushbackReader. (io/reader file))]
      (loop [out nil]
        (if-let [form (edn/read {:eof nil} reader)]
          (recur form)
          out)))))
