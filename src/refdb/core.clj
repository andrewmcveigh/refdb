(ns refdb.core
  "### The core namespace for the refdb library."
  (:refer-clojure :exclude [get find])
  (:require
   [clojure.core.reducers :as r]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [riddley.walk :as walk]))

(defn join-path [& args]
  {:pre [(every? (comp not nil?) args)]}
  (let [ensure-no-delims #(string/replace % #"(?:^/)|(?:/$)" "")]
    (str (when (.startsWith (first args) "/") \/)
         (string/join "/" (map ensure-no-delims args)))))

(defn coll-file [{:keys [path] :as db-spec} coll-name]
  {:pre [path]}
  (io/file path (format "%s.clj" coll-name)))

(defn meta-file [{:keys [path] :as db-spec} coll-name]
  {:pre [path]}
  (io/file path (format "%s.meta.clj" coll-name)))

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

(defn init! [{:keys [colls no-write?] :as db-spec} & {:keys [only]}]
  (doseq [[_ {:keys [coll-ref meta-file coll-file]}]
          (if only (select-keys colls only) colls)]
    (when (and (not no-write?) (not (.exists coll-file)))
      (spit coll-file ""))
    (when (and (not no-write?) (not (.exists meta-file)))
      (spit meta-file ""))
    (when (.exists meta-file)
      (dosync
       (ref-set coll-ref (load-file (.getCanonicalPath meta-file)))
       (when (.exists coll-file)
         (with-open [reader (java.io.PushbackReader. (io/reader coll-file))]
           (loop []
             (when-let [{:keys [id] :as form} (edn/read {:eof nil} reader)]
               (do (alter coll-ref update-in [:items id] (fnil conj (list)) form)
                   (recur))))))))))

(defn spit-record [coll-file record]
  (spit coll-file
        (str (when (meta record) (str \^ (pr-str (meta record)) \space))
             (prn-str record))
        :append true))

(defn write!
  "Persists `coll` to permanent storage."
  ([{:keys [no-write? path] :as db-spec} coll]
     {:pre [(or no-write? path)]}
     (write! db-spec coll nil))
  ([{:keys [no-write? path colls] :as db-spec} coll record]
     {:pre [(or no-write? path)]}
     (when-not no-write?
       (let [{:keys [coll-file meta-file coll-ref]} (coll colls)]
         (.mkdir (io/file path))
         (spit meta-file (pr-str (dissoc @coll-ref :items)))
         (if record
           (spit-record coll-file record)
           (do (spit coll-file "")
               (doall
                (map (partial spit-record coll-file)
                     (apply concat (vals (:items @coll-ref)))))))))))

(defn funcall? [sexpr]
  (and (sequential? sexpr)
       (or (= `with-transaction (first sexpr))
           (#{'let* 'let 'do 'if} (first sexpr))
           (and (symbol? (first sexpr))
                (or (fn? (first sexpr))
                    (resolve (first sexpr)))))))

(defn quote-sexprs [coll]
  (walk/walk-exprs
   funcall?
   (fn [expr]
     (cond (#{'let* 'let 'if} (first expr))
           (apply list (concat (take 2 expr) (quote-sexprs (drop 2 expr))))
           (= `with-transaction (first expr))
           (quote-sexprs (first (drop 3 expr)))
           :else
           (apply list 'list
                  (map #(cond (or (= 'do %)
                                  (and (symbol? %)
                                       (or (fn? %) (resolve %))))
                              (if (resolve %)
                                (let [m (meta (resolve %))]
                                  (list 'quote
                                        (symbol (name (ns-name (:ns m)))
                                                (name (:name m)))))
                                (list 'quote %))
                              (funcall? %)
                              (quote-sexprs %)
                              :else %)
                       expr))))
   #{`with-transaction}
   coll))

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
  (let [t (:transaction (meta record))]
    (with-open [r (java.io.PushbackReader.
                   (io/reader
                    (io/file (join-path (transaction-dir db-spec)
                                        (transaction-file t)))))]
      (loop [form (edn/read {:eof nil} r)]
        (when form
          (if (= (:id form) t) form (recur (edn/read {:eof nil} r))))))))

(defn destroy!
  "Resets the `coll`, and the file associated."
  [{:keys [no-write? path colls] :as db-spec} coll]
  (do (dosync (ref-set (:coll-ref (coll colls)) {}))
      (write! db-spec coll)))

(defn get-id
  "Gets a the next available ID for the database."
  [coll]
  (dosync (alter coll update-in [:last-id] (fnil inc -1)))
  (:last-id @coll))

(defn get
  "Gets an item from the collection by id."
  [coll id]
  (let [match (-> @coll (get-in [:items id]) first)]
    (when-not (::deleted match) match)))

(defn created [coll id]
  (let [match (-> @coll (get-in [:items id]) last)]
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

(defn find
  "Finds an item, or items in the collection by predicate. The predicate should
  be a map of `{:keyname \"wanted value\"}`. The default query operation is
  `?and`, however specifying separate level `?and`/`?or` operations is
  possible. E.G.,

    => (find coll
             (?or (?and {:first-name \"Benjamin\"
                         :surname \"Netanyahu\"})
                  (?and {:first-name \"Kofi\"
                         :surname\"Annan\"})))

  If the predicate is `nil` or empty `{}`, returns all items."
  ([coll pred]
     (remove ::deleted
             (filter (partial pred-match? pred)
                     (map (comp first val) (:items @coll)))))
  ([coll k v & kvs]
     (find coll (apply hash-map (concat [k v] kvs)))))

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

(defmacro with-transaction
  "Wraps `body` in a named transaction `t`. Will subsume inner transactions.
Provides the dosync implementation for save!.

Each body form should evaluate to satisfy map?, and the map should
contain at least one keyval:

    :sync (form ...)

Optional keyvals are:

    :pre (form ...)
    :post (form ...)

which are run before, and after the dosync :sync block respectively.

E.G.,

    (with-transaction retire
      (save! employee {:id ... :retired (java.util.Date.)})
      (save! employee {:id ... :active nil})
      (save! account {:id ... :employee ... :status :retired})
      {:pre (set-up ...)
       :sync (alter account update-in [:items id] conj nil)
       :post (doseq [x (file-seq (io/resource (format \"%s/images\" id)))]
               (.delete x))})
"
  [db-spec t & body]
  `(let [~'t2 ~(if (and (sequential? t) (= 'gensym (first t))) t `'~t)
         ~'body2 ~(vec (quote-sexprs body))
         before# (mapv :pre ~'body2)
         sync# (mapv :sync ~'body2)
         after# (mapv :post ~'body2)
         trans# (with-meta
                  {:name (name ~'t2)
                   :meta ~(meta t)
                   :inst (java.util.Date.)
                   :id (java.util.UUID/randomUUID)
                   :pre before#
                   :sync sync#
                   :post after#
                   }
                  {:type ::transaction})]
         (prn trans#)
     (-> trans#
         (update-in [:pre] eval)
         (update-in [:sync] #(dosync (eval %)))
         (update-in [:post] eval)
         (assoc :ok (write-transaction! ~db-spec trans#))
         :sync)))

(defn validate [db-spec coll m]
  (when-let [meta (some-> db-spec :colls coll :var meta)]
    (when-let [schema (when meta (::schema meta))]
      ((::validate meta) schema m))))

(defn save!
  "Saves item(s) `m` to `coll`."
  [{:keys [colls] :as db-spec} coll m]
  (assert (map? m) "Argument `m` must satisfy map?.")
  (assert (keyword? coll) "Argument `coll` must be a keyword, naming the coll.")
  (validate db-spec coll m)
  (let [{:keys [var coll-ref name coll-file meta-file]} (coll colls)
        exists? (and (:id m) (get coll-ref (:id m)))
        id (or (:id m) (get-id coll-ref))
        m (assoc m :id id :inst (java.util.Date.))]
    (dosync
     (alter coll-ref update-in [:items id] (fnil conj (list)) m)
     (when-not exists?
       (if (integer? id)
         (alter coll-ref update-in [:last-id] (fnil max 0) id))
       (alter coll-ref update-in [:count] (fnil inc 0))))
    (write! db-spec coll m)
    m))

(defn delete! [db-spec coll m]
  {:pre [(:id m)]}
  (save! db-spec coll (assoc m ::deleted (java.util.Date.))))

(defn fupdate-in
  ([m [k & ks] f & args]
   (if ks
     (assoc m k (apply fupdate-in (clojure.core/get m k) ks f args))
     (assoc m k (let [coll (clojure.core/get m k)]
                  (conj coll (apply f (first coll) args)))))))

(defn update!
  "'Updates' item with id `id` by applying fn `f` with `args` to it. E.G.,

    => (update! coll 3 update-in [:key1 0 :key2] assoc :x \"string content\")"
  [{:keys [colls] :as db-spec} coll id f & args]
  {:pre [(fn? f) (integer? id)]}
  (let [{:keys [name coll-ref]} (coll colls)
        exists? (get coll-ref id)]
    (validate db-spec coll (apply f exists? args))
    (dosync
     (alter coll-ref (partial apply fupdate-in) [:items id] f args)
     (alter coll-ref fupdate-in [:items id]
            assoc
            :id id
            :inst (java.util.Date.))
     (when-not exists?
       (if (integer? id)
         (alter coll-ref update-in [:last-id] (fnil max 0) id))
       (alter coll-ref update-in [:count] (fnil inc 0))))
    (write! db-spec coll (get coll-ref id))))

(defn history
  "Returns `n` items from the history of the record. If `n` is not specified,
  all history is returned"
  ([coll {id :id :as record} n]
   (let [past (next (get-in @coll [:items id]))]
     (if n (take n past) past)))
  ([coll record]
   (history coll record nil)))

(defn previous
  "Returns the `nth` last historical value for the record. If `n` is not
  specified, the latest historical value before the current value of the
  record. If record is a historical value, previous will return the latest
  `nth` value before it."
  ([coll record n]
   (nth (history coll record) n))
  ([coll record]
   (previous coll record 0)))

(defmacro db-spec [{:keys [path no-write?] :as opts} colls]
  `(let [path# (cond (string? ~path)
                     (if-let [path# (io/resource ~path)]
                       (io/file path#)
                       (io/file ~path))
                     (instance? java.net.URI ~path)
                     (io/file ~path)
                     :default ~path)
         opts# (assoc ~opts :path path#)]
     (assert (or path# ~no-write?)
             "Option `path`, or :no-write? must be specified.")
     (assert (or (and (instance? java.io.File path#) (.exists path#))
                 ~no-write?)
             "If `no-write?` not specified, option `path` must either
            be, or convert to a java.io.File, and it must exist.")
     (assoc opts#
       :colls (->> ~colls
                   (mapv #(let [var# (resolve %)]
                            (assoc {:name (name %)}
                              :coll-ref @var#
                              :var var#)))
                   (map #(let [n# (:name %)]
                           [(keyword n#)
                            (if ~no-write?
                              %
                              (assoc %
                                :coll-file (coll-file opts# n#)
                                :meta-file (meta-file opts# n#)))]))
                   (into {})))))
