(ns refdb.migrate
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [refdb.core :as db]))

(defn spit-history [{{n :count} :history :as x} dir {:keys [id] :as record}]
  (let [count (or n 0)
        hist-dir (-> dir (io/file (str id)) (io/file "history"))
        record (assoc-in record [:history :count] (inc count))]
    (when-not (.exists hist-dir) (.mkdirs hist-dir))
    (when x (spit (io/file hist-dir (pr-str count)) x))
    (spit (-> dir (io/file (str id)) (io/file "current")) record)
    record))

(defn cp-r [source dest]
  (let [ch-path (fn [source dest x]
                  (.toFile
                   (.resolve (.toPath dest)
                             (.relativize (.toPath source) (.toPath x)))))
        coll (->> (file-seq source)
                 (remove #{source})
                 (map (juxt identity (partial ch-path source dest))))
        dirs (filter #(.isDirectory (first %)) coll)
        files (remove #(.isDirectory (first %)) coll)]
    (when-not (.exists dest) (.mkdirs dest))
    (doseq [[_ dir] dirs] (.mkdirs dir))
    (doseq [[old new] files] (io/copy old new))))

(defmulti migrate! (fn [[from to] & _] [from to]))

(defmethod migrate! ["0.5" "0.6"]
  [_ {:keys [collections no-write? path schema-version] :as db-spec}]
  (println "Performing migration from refdb v0.5 to v0.6")
  (let [tmp (io/file path "_0.5")]
    (println (format "Backing up data to %s\n" tmp))
    (cp-r path tmp))
  (doseq [[_ {:keys [coll-ref meta-file coll-file coll-dir name] :as coll}]
          collections]
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
         (spit (io/file coll-dir "meta")
               {:items (set ids) :count (count ids)
                :schema-version schema-version}))
       (.delete coll-file)
       (.delete meta-file))))
  (spit (io/file path "meta")
        {:version "0.6" :collections (set (keys collections))
         :schema-version schema-version}))

(defn migrate-spec! [{:keys [version] :as db-spec}]
  (if (= version db/refdb-version)
    db-spec
    (do
      (migrate! [version db/refdb-version] db-spec)
      (apply db/db-spec
             (dissoc db-spec :collections :version)
             (keys (:collections db-spec))))))

(defn migrate-schema!
  [{:keys [collections path schema-version] :as db-spec} collection transf]
  (println (format "Performing schema-migration to version %s" schema-version))
  (let [tmp (io/file path (format "_to-schema-%s" schema-version))]
    (println (format "Backing up data to %s\n" tmp))
    (cp-r path tmp))
  (let [{:keys [meta-file coll-dir] meta-atom :meta} (collections collection)]
    (doseq [{:keys [id] :as x} (->> (db/find db-spec collection nil)
                                    (sort-by :id))]
      (let [history (doall (db/history db-spec collection x))
            current (transf x)
            record-dir (io/file coll-dir (str id))
            hist-dir (-> record-dir (io/file "history"))]
        (when-not (.exists record-dir) (.mkdirs record-dir))
        (when-not (.exists hist-dir) (.mkdirs hist-dir))
        (db/spit-record (-> record-dir (io/file "current")) current)
        (spit meta-file @meta-atom)
        (doseq [h history]
          (let [h (transf h)]
            (db/spit-record
             (io/file hist-dir (str (-> h meta :history :count))) h)))))))
