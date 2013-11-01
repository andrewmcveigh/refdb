(ns refdb.core
  "### The core namespace for the refdb library."
  (:refer-clojure :exclude [get find])
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [riddley.walk :as walk]))

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
   {:pre [(bound? #'*path*)]}
   (.mkdir (io/file *path*))
   (spit (meta-file coll-name) (pr-str (dissoc @coll :items)))
   (if record
     (spit (coll-file coll-name) (prn-str record) :append true)
     (do (spit (coll-file coll-name) "")
         (doall
           (map (comp #(spit (coll-file coll-name) % :append true) prn-str)
                (apply concat (vals (:items @coll)))))))))

(defn- transaction-file [transaction] "transaction.clj")

(defn write-transaction!
  "Writes a `transaction` to durable storage."
  [{:keys [id inst name] :as transaction}]
  {:pre [(bound? #'*path*)
         (= ::transaction (type transaction))
         (map? transaction)
         (and id inst name)]}
  (let [transaction-dir (join-path *path* "_transaction")]
    (try
      (.mkdir (io/file transaction-dir))
      (spit (io/file (join-path transaction-dir (transaction-file transaction)))
            (prn-str transaction)
            :append true)
      true
      (catch Exception _))))

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

(defn ->meta [m kw]
  (if (contains? m kw)
    (-> m
        (vary-meta (fnil assoc {}) kw (kw m))
        (dissoc kw))
    m))

(defn get
  "Gets an item from the collection by id."
  [coll id]
  (-> @coll
      (get-in [:items id])
      first
      (->meta :transaction)))

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
   (map (comp #(->meta % :transaction) first)
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

(def test-coll (ref nil))

(defn quote-sexprs [coll]
  (let [funcall? #(and (sequential? %)
                       (or (= `with-transaction (first %))
                           (#{'let* 'let 'do} (first %))
                           (and (symbol? (first %))
                                (or (fn? (first %))
                                    (resolve (first %))))))]
    (walk/walk-exprs
     funcall?
     (fn [expr]
       (cond (#{'let* 'let} (first expr))
             (apply list (concat (take 2 expr) (quote-sexprs (drop 2 expr))))
             (= `with-transaction (first expr))
             (quote-sexprs (first (drop 2 expr)))
             :else
             (apply list 'list
                    (map #(cond (or (= 'do %)
                                    (and (symbol? %)
                                         (or (fn? %) (resolve %))))
                                (list 'quote %)
                                (funcall? %)
                                (quote-sexprs %)
                                :else %)
                         expr))))
     #{`with-transaction}
     coll)))

(defmacro with-transaction [t & body]
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
  "Saves item(s) `m` to `coll`."
  ([coll m]
     `(let [m# ~m
            _# (assert (map? m#) "Argument `m` must satisfy map?.")
            exists?# (and (:id m#) (get ~coll (:id m#)))
            id# (or (:id m#) (get-id ~coll))
            m# (assoc ~m
                 :id id#
                 :inst (java.util.Date.))]
        (with-transaction (gensym "refdb-save!*_")
          (let [m# (assoc m# :transaction ~'transaction)]
            {:sync (do
                     (alter ~coll update-in [:items id#] (fnil conj (list)) m#)
                     (when-not exists?#
                       (alter ~coll update-in [:last-id] (fnil max 0) id#)
                       (alter ~coll update-in [:count] (fnil inc 0)))
                     m#)
             :post (write! ~coll ~(name coll) m#)}))))
  ([coll m & more]
     `(doall (map #(save! ~coll %) (conj ~m ~more)))))

;; (with-refdb-path "/tmp/"
;;   (with-transaction ^{:thing 1 :other 0} retire
;;     (save! test-coll {:test 0})
;;     (save! test-coll {:nothing 2})))

;; (let [xx 888]
;;   (with-transaction retire
;;     {:sync (mapv inc [1 2 3 4 xx])}
;;     {:sync (mapv inc [2 3 4 5])}))

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
   (let [past (map #(->meta % :transaction) (next (get-in @coll [:items id])))]
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
