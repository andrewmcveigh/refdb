(ns refdb.core
  (:refer-clojure :exclude [get find])
  (:require
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

(defn literal? [x]
  (or (true? x) (false? x) (keyword? x) (string? x) (number? x)))

(defn regex? [x]
  (instance? java.util.regex.Pattern x))

(defmacro init! [coll]
  `(let [file# (coll-file ~(name coll))]
     (when (.exists file#)
       (dosync (ref-set ~coll (load-file (.getCanonicalPath file#)))))))

(defn write! [coll coll-name]
  {:pre [(bound? #'*path*)]}
  (do (.mkdir (io/file *path*))
      (spit (coll-file coll-name) (pr-str @coll))))

(defmacro destroy! [coll]
  `(do (dosync (ref-set ~coll {}))
       (write! ~coll ~(name coll))))

(defn get-id [coll]
  (dosync (alter coll update-in [:last-id] (fnil inc -1)))
  (:last-id @coll))

(defn get [coll id]
  (get-in @coll [:items id]))

(defn any-pred-match? [pred item]
  (if (seq pred)
    (some (fn [[k v]]
            (let [value (k item)]
              (cond (literal? v) (= value v)
                    (regex? v) (re-seq v value)
                    :else (v (k item)))))
          pred)
    true))

(defn find
  ([coll pred]
   (filter (partial any-pred-match? pred) (vals (:items @coll))))
  ([coll k v & kvs]
   (find coll (apply hash-map (concat [k v] kvs)))))

(defn save!* [coll coll-name m]
  {:pre [(map? m)]}
  (let [id (or (:id m) (get-id coll))
        m (assoc m :id id)
        exists? (get coll id)]
    (dosync
      (alter coll assoc-in [:items id] m)
      (when-not exists?
        (alter coll update-in [:last-id] (fnil max 0) id)
        (alter coll update-in [:count] (fnil inc 0))))
    (write! coll coll-name)
    m))

(defn update! [coll coll-name f [id & path] & args]
  {:pre [(fn? f) (integer? id) (sequential? path)]}
  (let [exists? (get coll id)]
    (dosync
      (apply alter coll f (concat [:items id] path) args)
      (when-not exists?
        (alter coll update-in [:last-id] (fnil max 0) id)
        (alter coll update-in [:count] (fnil inc 0))))
    (write! coll coll-name)
    id))

(defmacro save!
  ([coll m]
   `(save!* ~coll ~(name coll) ~m))
  ([coll f path & args]
   `(update! ~coll ~(name coll) ~f ~path ~@args)))

(defmacro with-refdb-path [path-to-files & body]
  `(binding [*path* (if (instance? java.io.File ~path-to-files)
                      (.getCanonicalPath ~path-to-files)
                      ~path-to-files)]
     ~@body))

(defn wrap-refdb [handler path-to-files]
  (fn [request]
    (with-refdb-path path-to-files (handler request))))
