(ns refdb.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer :all]
   [clojure.set :as set]
   [refdb.core :as db]
   [schema.core :as s]))

(defn db-spec []
  (let [path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
    (.mkdirs (io/file path))
    (db/db-spec {:path path :version "0.6"} :coll1)))

(deftest all-test
  (let [db-spec (db-spec)]
    (dosync (ref-set (db/dbref db-spec :coll1) nil))
    (is (nil? (db/init! db-spec)))
    (db/save! db-spec :coll1 {:m 2})
    (let [res (first (db/find db-spec :coll1 {:m 2}))]
      (is (= 2 (:m res)))
      (db/save! db-spec :coll1 (assoc res :t 6))
      (is (= 6 (:t (first (db/find db-spec :coll1 {:m 2}))))))
    (db/destroy! db-spec :coll1)
    ;; (is (nil? (load-file (str (-> db-spec :collections :coll1 :coll-file)))))
    (is (= :coll1
           (:coll
            (try
              (db/update! db-spec :coll1 1 assoc-in [:test1 :test2 :thsteh] 2)
              (catch Exception e (ex-data e))))))
    (db/save! db-spec :coll1 {:id 1})
    (db/save! db-spec :coll1 {:id 3})
    (db/update! db-spec :coll1 1 assoc-in [:test1 :test2 :thsteh] 2)
    (db/update! db-spec :coll1 3 assoc-in [:test :test2 :thsteh] 2)
    (is (= {:id 3 :test {:test2 {:thsteh 2}}}
           (dissoc (db/get db-spec :coll1 3) :inst :history)))))

(deftest and-or-test
  (let [db-spec (db-spec)]
    (dosync (ref-set (db/dbref db-spec :coll1) nil))
    (is (nil? (db/init! db-spec)))
    (db/save! db-spec :coll1 {:m 1})
    (db/save! db-spec :coll1 {:m 2 :f 1})
    (db/save! db-spec :coll1 {:m 3 :f 2 :e 4})
    (db/save! db-spec :coll1 {:m 4 :f 3 :e 5})
    (is (= 5 (:e (first (db/find db-spec :coll1 (db/?and {:m 4 :f 3}))))))
    (is (empty? (db/find db-spec :coll1 (db/?and {:m 3 :f 3}))))
    (is (= 2 (count (db/find db-spec :coll1 (db/?or {:m 3 :f 3})))))
    (is (= 2 (count (db/find db-spec :coll1 (db/?or (db/?and {:m 4 :f 3})
                                                    (db/?and {:m 2 :f 1}))))))
    (is (= [{:m 3 :f 2 :e 4} {:m 4 :f 3 :e 5}]
           (sort-by :m (map #(dissoc % :id :inst :history)
                            (db/find db-spec :coll1
                                     (db/?and (db/?or {:m 4 :f 2})
                                              (db/?or {:e 5 :m 3})))))))
    (db/destroy! db-spec :coll1)))

(deftest history-test
  (let [db-spec (db-spec)]
    (dosync (ref-set (db/dbref db-spec :coll1) nil))
    (is (nil? (db/init! db-spec)))
    (let [{id :id :as saved} (db/save! db-spec :coll1 {:m 1})]
      (db/save! db-spec :coll1 (assoc saved :a 2))
      (db/save! db-spec :coll1 (assoc saved :b 3))
      (is (= [{:id id :m 1 :a 2} {:id id :m 1}]
             (map #(dissoc % :inst :history)
                  (db/history db-spec :coll1 saved))))
      (is (= {:m 1 :a 2 :id id}
             (dissoc (db/previous db-spec :coll1 saved) :inst :history)))
      (dosync (ref-set (db/dbref db-spec :coll1) nil))
      (db/init! db-spec)
      (is (= [{:id id :m 1 :a 2} {:id id :m 1}]
             (map #(dissoc % :inst :history)
                  (db/history db-spec :coll1 (first (db/find db-spec :coll1 nil))))))
      (db/save! db-spec :coll1 {:ttt 33 :ff 88}))
    (db/destroy! db-spec :coll1)))

(def TypeX
  {:x1 s/Int
   :x2 [s/Int]})

(deftest validate-test
  (let [path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))
        _ (.mkdirs (io/file path))
        db-spec (db/db-spec {:path path :version "0.6"}
                            (db/with-schema :coll-x TypeX s/validate))]
    (db/init! db-spec)
    (is (db/save! db-spec :coll-x {:x1 10 :x2 [15 20]}))
    (is (:error
         (try
           (db/save! db-spec :coll-x {:x1 "string" :x2 [15 20]})
           (catch Exception e (ex-data e)))))
    (is (:error
         (try
           (db/save! db-spec :coll-x {:x 10 :x2 [15 20]})
           (catch Exception e (ex-data e)))))))
