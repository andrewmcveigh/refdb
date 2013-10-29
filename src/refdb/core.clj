(ns refdb.core
  "### The core namespace for the refdb library."
  (:refer-clojure :exclude [get find])
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as string]))

(declare ^:dynamic *path*)

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

(defn write!
  "Persists `coll` to permanent storage."
  ([coll coll-name]
   {:pre [(bound? #'*path*)]}
   (write! coll coll-name nil))
  ([coll coll-name record]
   (.mkdir (io/file *path*))
   (spit (meta-file coll-name) (pr-str (dissoc @coll :items)))
   (if record
     (spit (coll-file coll-name) (prn-str record) :append true)
     (do (spit (coll-file coll-name) "")
         (doall
           (map (comp #(spit (coll-file coll-name) % :append true) prn-str)
                (apply concat (vals (:items @coll)))))))))

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
  (first (get-in @coll [:items id])))

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
                 (let [value (k item)]
                   (cond (literal? v) (= value v)
                         (regex? v) (re-seq v value)
                         :else (v (k item)))))
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
   (map first
        (filter (comp (partial pred-match? pred) first) (vals (:items @coll)))))
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

(defn save!* [coll coll-name m]
  {:pre [(map? m)]}
  (let [id (or (:id m) (get-id coll))
        m (assoc m
                 :id id
                 :inst (java.util.Date.))
        exists? (get coll id)]
    (dosync
      (alter coll update-in [:items id] (fnil conj (list)) m)
      (when-not exists?
        (alter coll update-in [:last-id] (fnil max 0) id)
        (alter coll update-in [:count] (fnil inc 0))))
    (write! coll coll-name m)
    m))

{:meta {:action :retire}
 :sync (fn []
         (alter coll update-in [:items id] (fnil conj (list)) m)
         (when-not exists?
           (alter coll update-in [:last-id] (fnil max 0) id)
           (alter coll update-in [:count] (fnil inc 0))))
 :after (write! coll coll-name m)
 :inst (java.util.Date.)}

(defmacro save!
  "Saves item(s) `m` to `coll`."
  ([coll m]
   `(cond (map? ~m)
          (save!* ~coll ~(name coll) ~m)
          (sequential? ~m)
          (doall (map (partial save!* ~coll ~(name coll)) ~m))
          :else (throw (ex-info "Argument `m` must be either a map, or a sequential collection."
                                {:type ::invalid-argument :m ~m}))))
  ([coll m & more]
   `(save! (conj ~m ~more))))

(defmacro delete! [coll m]
  {:pre [(:id m)]}
  `(save! ~'coll ~(assoc m :deleted (java.util.Date.))))

(defn fupdate-in
  ([m [k & ks] f & args]
   (if ks
     (assoc m k (apply fupdate-in (clojure.core/get m k) ks f args))
     (assoc m k (let [coll (clojure.core/get m k)]
                  (conj coll (apply f (first coll) args)))))))

(defn update!* [coll coll-name id f path & args]
  {:pre [(fn? f) (integer? id) (sequential? path)]}
  (let [exists? (get coll id)]
    (dosync
      (apply alter coll fupdate-in [:items id] f path args)
      (alter coll fupdate-in [:items id] assoc :id id :inst (java.util.Date.))
      (when-not exists?
        (alter coll update-in [:last-id] (fnil max 0) id)
        (alter coll update-in [:count] (fnil inc 0))))
    (let [m (get coll id)]
      (write! coll coll-name m)
      m)))

(defmacro update!
  "'Updates' item with id `id` by applying fn `f` with `args` to it. E.G.,

    => (update! coll 3
                update-in [:key1 0 :key2] assoc :x \"string content\")"
  [coll id f path & args]
  `(update!* ~coll ~(name coll) ~id ~f ~path ~@args))

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

(defn history
  "Returns `n` items from the history of the record. If `n` is not specified,
  all history is returned"
  ([coll {id :id :as record} n]
   (let [past (next (get-in @coll [:items id]))]
     (if n
       (take n past)
       past)))
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
