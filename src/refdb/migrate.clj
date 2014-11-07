(ns refdb.migrate
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]))

(defn history-spec [{:keys [coll-dir name]}]
  (let [dir (-> coll-dir (io/file "history"))]
    (when-not (.exists dir) (.mkdirs dir))
    {:dir dir
     :count (count (.listFiles dir))}))

(defn spit-history [{{n :count} :history :as x} dir {:keys [id] :as record}]
  (let [count (or n 0)
        hist-dir (-> dir (io/file id) (io/file "history"))
        record (assoc-in record [:history :count] (inc count))]
    (when-not (.exists hist-dir) (.mkdirs hist-dir))
    (when x (spit (io/file hist-dir (pr-str count)) x))
    (spit (-> dir (io/file id) (io/file "current")) record)
    record))

(defmulti migrate! (fn [[from to] & _] [from to]))

(defmethod migrate! [0.5 0.6]
  [_ {:keys [collections no-write? path] :as db-spec} & {:keys [only]}]
  (doseq [[_ {:keys [coll-ref meta-file coll-file coll-dir name] :as coll}]
          (if only (select-keys collections only) collections)]
    (when-not (.exists coll-dir) (.mkdirs coll-dir))
    (when (and meta-file (.exists meta-file))
      (dosync
       (ref-set coll-ref {})
       (when (.exists coll-file)
         (with-open [reader (java.io.PushbackReader. (io/reader coll-file))]
           (loop []
             (when-let [{:keys [id] :as x} (edn/read {:eof nil} reader)]
               (do
                 (when (integer? id)
                   (alter coll-ref update-in [:last-id] (fnil max 0) id))
                 (when-not (get-in @coll-ref [:items id])
                   (alter coll-ref update-in [:count] (fnil inc 0)))
                 (alter coll-ref update-in [:items id] spit-history coll-dir x)
                 (recur))))))
       (let [ids (keys (:items @coll-ref))]
         (spit (io/file coll-dir "meta") {:items (set ids) :count (count ids)}))
       (.delete coll-file)
       (.delete meta-file))))
  (spit (io/file path "meta")
        {:version 0.6 :collections (set (keys collections))}))
