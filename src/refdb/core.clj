(ns refdb.core
  "### The core namespace for the refdb library."
  (:refer-clojure :exclude [get find])
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [refdb.internal.core :refer [load-form]]
   [refdb.migrate :as migrate]))

(def refdb-version "0.6")

(defrecord Collection [coll-file meta-file coll-ref name])

(defrecord RefDB [path collections])

(defmethod print-dup RefDB [{:keys [path collections version]} w]
  (.write w (format "#<RefDB:
  {:path %s
   :version %s
   :collections %s}>"
                    (pr-str path)
                    (pr-str version)
                    (pr-str (map key collections)))))

(defmethod print-method RefDB [{:keys [path collections version]} w]
  (.write w (format "#<RefDB:
  {:path %s
   :version %s
   :collections %s}>"
                    (pr-str path)
                    (pr-str version)
                    (pr-str (map key collections)))))

(defn db-version [{:keys [version] :as db-spec}]
  (assert version "No version found, assume < 6.0")
  version)

(defn join-path [& args]
  {:pre [(every? (comp not nil?) args)]}
  (let [ensure-no-delims #(string/replace % #"(?:^/)|(?:/$)" "")]
    (str (when (.startsWith (first args) "/") \/)
         (string/join "/" (map ensure-no-delims args)))))

(defn- transaction-dir [{:keys [path] :as db-spec}]
  (join-path path "_transaction"))

(defn- transaction-file [transaction] "transaction.clj")

(defn literal?
  "Is `x` a literal value, I.E, is `x` a `string`, `number`, `keyword` or
  `true`/`false`."
  [x]
  (or (true? x) (false? x) (keyword? x) (string? x) (number? x)))

(defn regex?
  "Is `x` a regex pattern?"
  [x]
  (instance? java.util.regex.Pattern x))

(defn dbref [db-spec coll]
  (-> db-spec :collections coll :coll-ref))

(defn with-schema [coll-kw schema valid-fn]
  (map->Collection
   {:name (name coll-kw)
    :key coll-kw
    ::schema schema
    ::validate valid-fn}))

(defn coerce-coll [x]
  (if (keyword? x) (map->Collection {:name (name x) :key x}) x))

(defmulti collection (fn [{:keys [version]} & _] version))

(defmethod collection "0.6"
  [{:keys [path no-write? version]} {:keys [name key] :as collection}]
  [key (merge collection
              {:coll-ref (ref nil)
               :meta (ref {:items #{} :count 0})}
              (when path
                {:coll-dir (io/file path name)
                 :meta-file (-> path (io/file name) (io/file "meta"))}))])

(defmethod collection "0.5"
  [{:keys [path no-write? version]} {:keys [name key] :as collection}]
  [key (merge collection
              {:coll-ref (ref nil)}
              (when path
                {:coll-file (io/file path (format "%s.clj" name))
                 :coll-dir (io/file path name)
                 :meta-file (io/file path (format "%s.meta.clj" name))}))])

(defn db-spec
  "Creates a db-spec. Takes a map of `opts`, `& collections`. `opts` must
  contain either `:path` or `:no-write` must be truthy. `:path` can be a
  `java.net.URI`, a `java.io.File`, or a `String`, and it must point to
  an existing file. `collections` should be passed as keywords which
  name the collections. E.G.,

    (db-spec {:path \"data\"} :cats :dogs)
"
  [{:keys [path no-write? version] :as opts} & collections]
  (let [path (cond (string? path)
                   (if-let [resource (io/resource path)]
                     (io/file resource)
                     (io/file path))
                   (instance? java.net.URI path)
                   (io/file path)
                   :default path)
        meta (io/file path "meta")
        {:keys [version] :as meta}
        (if (.exists meta)
          (load-form meta)
          {:version (or version "0.5") :collections (set (map :key collections))})
        opts (assoc opts :path path :version version)]
    (assert (or path no-write?)
            "Option `path`, or :no-write? must be specified.")
    (assert (or (and (instance? java.io.File path) (.exists path)) no-write?)
            "If `no-write?` not specified, option `path` must either
            be, or convert to a java.io.File, and it must exist.")
    (map->RefDB
     (assoc opts
       :collections
       (->> collections
            (map (comp (partial collection opts) coerce-coll))
            (into {}))))))

(defn init!
  "Initialize the collections in a db-spec.

    (db/init! db-spec)

... to initialize all collections in the `db-spec`, or...

    (db/init! db-spec :only #{:cats})

... to only initialize some of them."
  [{:keys [collections no-write? version] :as db-spec} & {:keys [only]}]
  (doseq [[_ {:keys [coll-ref meta-file meta coll-dir name]}]
          (if only (select-keys collections only) collections)]
    (when (and (not no-write?) (not (.exists coll-dir)))
      (.mkdirs coll-dir))
    (when (and (not no-write?) (not (.exists meta-file)))
      (spit meta-file (pr-str @meta)))
    (when (and meta-file (.exists meta-file))
      (dosync
       (ref-set meta (load-form meta-file))
       (doseq [id (:items @meta)]
         (let [coll-file (-> coll-dir (io/file (str id)) (io/file "current"))
               {:keys [id] :as form} (load-form coll-file)]
           (alter coll-ref assoc-in [:items id] form)))))))

(defn spit-record [file record]
  (spit file
        (str (when (meta record) (str \^ (pr-str (meta record)) \space))
             (prn-str record))))

(defn write!
  "Persists `coll` to permanent storage."
  [{{n :count} :history :as x} db-spec coll-key {:keys [id] :as record}]
  (let [{dir :coll-dir :keys [meta meta-file]} (-> db-spec :collections coll-key)
        count (or n 0)
        record (some-> record (assoc-in [:history :count] (inc count)))
        record-dir (io/file dir (str id))]
    (when-not (.exists record-dir) (.mkdirs record-dir))
    (when x
      (let [hist-dir (-> record-dir (io/file "history"))]
        (when-not (.exists hist-dir) (.mkdirs hist-dir))
        (spit-record (io/file hist-dir (pr-str count)) x)))
    (spit meta-file @meta)
    (spit-record (-> record-dir (io/file "current")) record)
    record))

(defn write-transaction!
  "Writes a `transaction` to durable storage."
  [{:keys [no-write? path] :as db-spec} {:keys [id inst name] :as transaction}]
  {:pre [(or no-write? path)
         (= ::transaction (type transaction))
         (map? transaction)
         (and id inst name)]}
  (when-not no-write?
    (let [transaction-dir (transaction-dir db-spec)]
      (try
        (.mkdir (io/file transaction-dir))
        (spit (io/file (join-path transaction-dir
                                  (transaction-file transaction)))
              (prn-str transaction)
              :append true)
        true
        (catch Exception _)))))

(defn transaction [{:keys [path] :as db-spec} record]
  "Returns the `record`'s transaction."
  {:pre [(:transaction (meta record))]}
  (let [t (:transaction (meta record))
        reader-opts {:eof nil :readers *data-readers*}]
    (with-open [r (java.io.PushbackReader.
                   (io/reader
                    (io/file (join-path (transaction-dir db-spec)
                                        (transaction-file t)))))]
      (loop [form (edn/read reader-opts r)]
        (when form
          (if (= (:id form) t) form (recur (edn/read reader-opts r))))))))

(defn get-id
  "Gets a the next available ID for the database."
  [db-spec coll]
  (let [coll (dbref db-spec coll)]
    (dosync (-> coll
                (alter update-in [:last-id] (fnil inc -1))
                (:last-id)))))

(defn get
  "Gets an item from the collection by id."
  [db-spec coll id]
  (let [match (-> @(dbref db-spec coll) (get-in [:items id]))]
    (when-not (::deleted match) match)))

(defn created [db-spec coll id] ; TODO: get first history item
  (let [match (-> @(dbref db-spec coll) (get-in [:items id]) last)]
    (when-not (::deleted match)
      (:inst match))))

(defn pred-match?
  "Returns truthy if the predicate, pred matches the item. If the predicate is
  `nil` or empty `{}`, returns `true`."
  [pred item]
  (let [?fn (condp = (::? (meta pred)) ::and every? ::or some every?)]
    (cond (or (nil? pred) (empty? pred)) true
          (vector? pred)
          (?fn #(pred-match? % item) pred)
          (map? pred)
          (?fn (fn [[k v]]
                 (let [value (if (vector? k) (get-in item k) (k item))]
                   (cond (literal? v) (= value v)
                         (regex? v) (when value (re-seq v value))
                         (and (set? v) (vector? value))
                         (or (v value) (some v value))
                         :else (v value))))
               pred)
          :default nil)))

(defn ?and
  "Creates `?and` operation predicate."
  ([pred]
   (with-meta pred {::? ::and}))
  ([head & tail]
   (with-meta `[~head ~@tail] {::? ::and})))

(defn ?or
  "Creates `?or` operation predicate."
  ([pred]
   (with-meta pred {::? ::or}))
  ([head & tail]
   (with-meta `[~head ~@tail] {::? ::or})))

(defn find
  "Finds an item, or items in the collection by predicate. The
  predicate should be a map of `{:keyname \"wanted value\"}`. The
  default query operation is `?and`, however specifying separate level
  `?and`/`?or` operations is possible. If the predicate is `nil` or
  empty `{}`, returns all items.  E.G.,

    (db/find db-spec :cats {:color \"orange\" :name \"Reg\"})

    => ({:id 457 :breed \"Tabby\" :color \"orange\" :name \"Reg\"}, ...)

  Predicates can match by `literal?` values, regexes and functions.

    (db/find db-spec :cats {:color #\"(orange)|(brown)\"})
    (db/find db-spec :cats {:color #(or (= % \"brown\") (= % \"orange))\"})

    => ({:id 457 :breed \"Tabby\" :color \"orange\" :name \"Reg\"}, ...)

  Predicates can have sub-maps, and sets can be used to partially match
  collections.

    (db/find db-spec :cats {:friends #{\"Tom\"}})

    => ({:id 457 :breed \"Persian\" :color \"Grey\" :name \"Bosco\" :friends [\"Tom\", \"Dick\", \"Harry\"]}, ...)

  You can also search deeper into a match using a vector as a key.

    (db/find db-spec :cats {[:skills :jumping :max-height] 20})

  By default a predicate's matching behavior for key-vals is AND. E.G.,

    (db/find db-spec :cats {:color \"orange\" :name \"Reg\"})

  finds cats with `:color` `\"orange\"` AND `:name` `\"Reg\"`. It's possible
  to specify that the predicate should use OR matching behavior, or a
  combination. E.G.,

    (require '[refdb.core :as db :refer [?and ?or]])

    (db/find db-spec :cats (?or (?and {:name \"Timmy\"
                                       :color \"Orange\"})
                                (?and {:friends #{\"Timmy\"}})))

"
  [db-spec coll pred]
  (->> (vals (:items @(dbref db-spec coll)))
       (filter (partial pred-match? pred))
       (remove ::deleted)))

(defmacro with-transaction [db-spec transaction & body]
  `(do
     (if (clojure.lang.LockingTransaction/isRunning)
       (do ~@body)
       (sync nil ~@body))
     (write-transaction!
      ~db-spec
      (with-meta
        (assoc ~transaction
          :inst (java.util.Date.)
          :id (java.util.UUID/randomUUID)
          :body '~body)
        {:type ::transaction}))))

(defn validate [db-spec coll m]
  (let [coll (some-> db-spec :collections coll)]
    (when-let [schema (::schema coll)]
      ((::validate coll) schema m))))

(defn save!
  "Saves item(s) `m` to `coll`."
  [db-spec coll m]
  (assert (map? m) "Argument `m` must satisfy map?.")
  (assert (keyword? coll) "Argument `coll` must be a keyword, naming the coll.")
  (validate db-spec coll m)
  (let [coll-ref (dbref db-spec coll)
        exists? (or (:exists? (meta m))
                    (and (:id m) (get db-spec coll (:id m))))
        id (or (:id m) (get-id db-spec coll))
        m (assoc m :id id :inst (java.util.Date.))]
    (dosync
     (when-not exists?
       (if (integer? id)
         (alter coll-ref update-in [:meta :last-id] (fnil max 0) id))
       (alter coll-ref update-in [:meta :count] (fnil inc 0)))
     (alter (-> db-spec :collections coll :meta) update-in [:items] conj id)
     (-> coll-ref
         (alter update-in [:items id] write! db-spec coll m)
         (get-in [:items id])))))

(defn update!
  "'Updates' item with id `id` by applying fn `f` with `args`. The
  item must exist in the collection to update it."
  [db-spec coll id f & args]
  {:pre [(fn? f) (or (integer? id) (string? id) (keyword? id))]}
  (let [exists? (get db-spec coll id)]
    (if exists?
      (save! db-spec coll (with-meta (apply f exists? args) {:exists? true}))
      (throw
       (ex-info "Item with `id` must exist in coll." {:coll coll :id id})))))

(defn delete! [db-spec coll m]
  {:pre [(:id m)]}
  (save! db-spec coll (assoc m ::deleted (java.util.Date.))))

(defn destroy!
  "Resets the `coll`, and the file associated."
  [db-spec coll]
  (do (dosync (ref-set (dbref db-spec coll) {}))
      (write! nil db-spec coll nil)))

(defn history
  "Returns `n` items from the history of the record. If `n` is not specified,
  all history is returned"
  [db-spec coll {id :id :as record} & [n]]
  (assert (not (:no-write? db-spec))
          "History does not work without durable storage")
  (let [coll-ref (dbref db-spec coll)
        dir (-> (get-in db-spec [:collections coll :coll-dir])
                (io/file (str id))
                (io/file "history"))
        past (some->> (get-in @coll-ref [:items id :history :count])
                      (range 1)
                      (reverse)
                      (map (fn [i] (load-form (io/file dir (str i))))))]
    (if n (take n past) past)))

(defn previous
  "Returns the `nth` last historical value for the record. If `n` is not
  specified, the latest historical value before the current value of the
  record. If record is a historical value, previous will return the latest
  `nth` value before it."
  ([db-spec coll record n]
   (nth (history db-spec coll record) n))
  ([db-spec coll record]
   (previous db-spec coll record 0)))
