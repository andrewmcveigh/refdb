(ns refdb.core
  "### The core namespace for the refdb library."
  (:refer-clojure :exclude [get find])
  (:require
   [clojure.core.reducers :as r]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [riddley.walk :as walk]))

(declare ^:dynamic *path*)
(def ^:dynamic *no-write* nil)

(defn join-path [& args]
  {:pre [(every? (comp not nil?) args)]}
  (let [ensure-no-delims #(string/replace % #"(?:^/)|(?:/$)" "")]
    (str (when (.startsWith (first args) "/") \/)
         (string/join "/" (map ensure-no-delims args)))))

(defn coll-file [coll-name]
  {:pre [(bound? #'*path*)]}
  (io/file (join-path *path* (format "%s.clj" coll-name))))

(defn meta-file [coll-name]
  {:pre [(bound? #'*path*)]}
  (io/file (join-path *path* (format "%s.meta.clj" coll-name))))

(defn- transaction-dir [] (join-path *path* "_transaction"))

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

(defn init!* [coll meta-file coll-file]
  (when (.exists meta-file)
    (dosync
      (ref-set coll (load-file (.getCanonicalPath meta-file)))
      (when (.exists coll-file)
        (with-open [reader (java.io.PushbackReader. (io/reader coll-file))]
          (loop []
            (when-let [{:keys [id] :as form} (edn/read {:eof nil} reader)]
              (do (alter coll update-in [:items id] (fnil conj (list)) form)
                  (recur)))))))))

(defmacro init!
  "Initializes the `coll` from the filesystem."
  [coll]
  `(init!* ~coll (meta-file ~(name coll)) (coll-file ~(name coll))))

(defn spit-record [coll-name coll-file record]
  (spit (coll-file coll-name)
        (str (when (meta record) (str \^ (pr-str (meta record)) \space))
             (prn-str record))
        :append true))

(defn write!
  "Persists `coll` to permanent storage."
  ([coll coll-name]
   {:pre [(bound? #'*path*)]}
   (write! coll coll-name nil))
  ([coll coll-name record]
   {:pre [(bound? #'*path*)]}
   (when-not *no-write*
     (.mkdir (io/file *path*))
     (spit (meta-file coll-name) (pr-str (dissoc @coll :items)))
     (if record
       (spit-record coll-name coll-file record)
       (do (spit (coll-file coll-name) "")
           (doall
            (map (partial spit-record coll-name coll-file)
                 (apply concat (vals (:items @coll))))))))))

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
           (quote-sexprs (first (drop 2 expr)))
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
  [{:keys [id inst name] :as transaction}]
  {:pre [(bound? #'*path*)
         (= ::transaction (type transaction))
         (map? transaction)
         (and id inst name)]}
  (let [transaction-dir (transaction-dir)]
    (try
      (.mkdir (io/file transaction-dir))
      (spit (io/file (join-path transaction-dir (transaction-file transaction)))
            (prn-str transaction)
            :append true)
      true
      (catch Exception _))))

(defn transaction [record]
  "Returns the `record`'s transaction."
  {:pre [(:transaction (meta record))]}
  (let [t (:transaction (meta record))]
    (with-open [r (java.io.PushbackReader.
                   (io/reader
                    (io/file (join-path (transaction-dir)
                                        (transaction-file t)))))]
      (loop [form (edn/read {:eof nil} r)]
        (when form
          (if (= (:id form) t) form (recur (edn/read {:eof nil} r))))))))

(defmacro destroy!
  "Resets the `coll`, and the file associated."
  [coll]
  `(do (dosync (ref-set ~coll {}))
       (write! ~coll ~(name coll))))

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
  [t & body]
  `(let [~'t2 ~(if (and (sequential? t) (= `gensym (first t))) t `'~t)
         ~'transaction (java.util.UUID/randomUUID)
         ~'body2 ~(vec (quote-sexprs body))
         before# (mapv :pre ~'body2)
         sync# (mapv :sync ~'body2)
         after# (mapv :post ~'body2)
         trans# (with-meta
                  {:name (name ~'t2)
                   :meta ~(meta t)
                   :inst (java.util.Date.)
                   :id ~'transaction
                   :pre before#
                   :sync sync#
                   :post after#}
                  {:type ::transaction})]
     (-> trans#
         (update-in [:pre] eval)
         (update-in [:sync] #(dosync (eval %)))
         (update-in [:post] eval)
         (assoc :ok (write-transaction! trans#))
         :sync)))

(defmacro save!
  "Saves item(s) `m` to `coll`. If not wrapped in a transaction, wraps
it's own."
  ([coll m]
     `(let [m# ~m
            _# (assert (map? m#) "Argument `m` must satisfy map?.")
            meta# (meta (resolve '~coll))
            schema# (when meta# (::schema meta#))
            _# (when schema# ((::validate meta#) schema# m#))
            exists?# (and (:id m#) (get ~coll (:id m#)))
            id# (or (:id m#) (get-id ~coll))
            m# (assoc ~m
                 :id id#
                 :inst (java.util.Date.))]
        (with-transaction (gensym "refdb-save!_")
          (let [m# (vary-meta m# (fnil assoc {}) :transaction ~'transaction)]
            {:sync (do
                     (alter ~coll update-in [:items id#] (fnil conj (list)) m#)
                     (when-not exists?#
                       (if (integer? id#)
                         (alter ~coll update-in [:last-id] (fnil max 0) id#))
                       (alter ~coll update-in [:count] (fnil inc 0)))
                     m#)
             :post (write! ~coll ~(name coll) m#)}))))
  ([coll m & more]
     `(doall (map #(save! ~coll %) ~(vec (cons m more))))))

(defmacro delete! [coll m]
  {:pre [`(:id ~m)]}
  `(save! ~coll (assoc ~m ::deleted (java.util.Date.))))

(defn fupdate-in
  ([m [k & ks] f & args]
   (if ks
     (assoc m k (apply fupdate-in (clojure.core/get m k) ks f args))
     (assoc m k (let [coll (clojure.core/get m k)]
                  (conj coll (apply f (first coll) args)))))))

(defmacro update!
  "'Updates' item with id `id` by applying fn `f` with `args` to it. E.G.,

    => (update! coll 3
                update-in [:key1 0 :key2] assoc :x \"string content\")

If not wrapped in a transaction, wraps it's own."
  [coll id f & args]
  {:pre [`(fn? ~f) `(integer? ~id)]}
  `(let [exists?# (get ~coll ~id)]
     (when-let [meta# ~(meta (resolve coll))]
       (when-let [schema# (::schema meta#)]
         ((::validate meta#) schema# (~f exists?# ~@args))))
     (with-transaction (gensym "refdb-update!_")
       {:sync (do
                (alter ~coll fupdate-in [:items ~id] ~f ~@args)
                (alter ~coll fupdate-in [:items ~id]
                       assoc
                       :id ~id
                       :inst (java.util.Date.))
                (when-not exists?#
                  (if (integer? ~id)
                    (alter ~coll update-in [:last-id] (fnil max 0) ~id))
                  (alter ~coll update-in [:count] (fnil inc 0)))
                (get ~coll ~id))
        :post (write! ~coll ~(name coll) (get ~coll ~id))})))

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

(defmacro with-refdb-path
  "Sets the file path context for `body` to operate in."
  [path-to-files & body]
  `(binding [*path* (if (instance? java.io.File ~path-to-files)
                      (.getCanonicalPath ~path-to-files)
                      ~path-to-files)]
     ~@body))

(defn wrap-refdb
  "Wraps the file path context in a ring middleware function."
  [handler path-to-files]
  (fn [request]
    (with-refdb-path path-to-files (handler request))))

(defmacro fixture [path & colls]
  `(fn [f#]
     (binding [*no-write* true]
       (with-refdb-path (io/file ~path)
         ~@(map #(list 'init! %) colls)
         (f#)))))
