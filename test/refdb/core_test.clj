(ns refdb.core-test
  (:require
    [clojure.test :refer :all]
    [clojure.set :as set]
    [refdb.core :as db]
    [riddley.walk :as walk]))

(def coll1 (ref nil))

(deftest all-test
  (let [path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
    (dosync (ref-set coll1 nil))
    (db/with-refdb-path path
      (is (nil? (db/init! coll1)))
      (db/save! coll1 {:m 2})
      (let [res (first (db/find coll1 {:m 2}))]
        (is (= 2 (:m res)))
        (db/save! coll1 (assoc res :t 6))
        (is (= 6 (:t (first (db/find coll1 {:m 2}))))))
      (db/destroy! coll1)
      (is (nil? (load-file (str (db/coll-file "coll1")))))
      (db/update! coll1 1 assoc-in [:test1 :test2 :thsteh] 2)
      (db/update! coll1 3 assoc-in [:test :test2 :thsteh] 2)
      (is (= {:id 3 :test {:test2 {:thsteh 2}}}
             (dissoc (db/get coll1 3) :inst))))))

(deftest and-or-test
  (let [path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
    (dosync (ref-set coll1 nil))
    (db/with-refdb-path path
      (is (nil? (db/init! coll1)))
      (db/save! coll1 {:m 1})
      (db/save! coll1 {:m 2 :f 1})
      (db/save! coll1 {:m 3 :f 2 :e 4})
      (db/save! coll1 {:m 4 :f 3 :e 5})
      (is (= 5 (:e (first (db/find coll1 (db/?and {:m 4 :f 3}))))))
      (is (empty? (db/find coll1 (db/?and {:m 3 :f 3}))))
      (is (= 2 (count (db/find coll1 (db/?or {:m 3 :f 3})))))
      (is (= 2 (count (db/find coll1 (db/?or (db/?and {:m 4 :f 3})
                                             (db/?and {:m 2 :f 1}))))))
      (is (= [{:m 3 :f 2 :e 4} {:m 4 :f 3 :e 5}]
             (sort-by :m (map #(dissoc % :id :inst)
                              (db/find coll1
                                       (db/?and (db/?or {:m 4 :f 2})
                                                (db/?or {:e 5 :m 3})))))))
      (db/destroy! coll1))))

(deftest history-test
  (let [path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
    (dosync (ref-set coll1 nil))
    (db/with-refdb-path path
      (is (nil? (db/init! coll1)))
      (let [{id :id :as saved} (first (db/save! coll1 {:m 1}))]
        (db/save! coll1 (assoc saved :a 2))
        (db/save! coll1 (assoc saved :b 3))
        (is (= [{:id id :m 1 :a 2} {:id id :m 1}]
               (map #(dissoc % :inst)
                    (db/history coll1 saved))))
        (is (= {:m 1 :a 2 :id id}
               (dissoc (db/previous coll1 saved) :inst)))
        (dosync (ref-set coll1 nil))
        (db/init! coll1)
        (is (= [{:id id :m 1 :a 2} {:id id :m 1}]
               (map #(dissoc % :inst)
                    (db/history coll1 (first (db/find coll1 nil))))))
        (db/with-transaction ^:test123 testtrans
          (db/save! coll1 {:ttt 33 :ff 88}))
        (dosync (ref-set coll1 nil))
        (db/init! coll1)
        (let [trns (db/transaction (first (db/find coll1 :ttt 33)))]
          (is (= "testtrans" (:name trns)))
          (is (= {:test123 true} (:meta trns)))))
      (db/destroy! coll1))))
