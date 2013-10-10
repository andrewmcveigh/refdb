(ns refdb.core-test
  (:require
    [clojure.test :refer :all]
    [refdb.core :as db]))

(deftest all-test
  (let [coll1 (ref nil)
        path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
    (db/with-refdb-path path
      (is (nil? (db/init! coll1)))
      (db/save! coll1 {:m 2})
      (let [res (first (db/find coll1 {:m 2}))]
        (is (= 2 (:m res)))
        (db/save! coll1 (assoc res :t 6))
        (is (= 6 (:t (first (db/find coll1 {:m 2}))))))
      (db/destroy! coll1)
      (is (= nil (load-file (str (db/coll-file "coll1")))))
      (db/save! coll1 assoc-in [3 :test :test2 :thsteh] 2)
      (is (= {:test {:test2 {:thsteh 2}}} (db/get coll1 3))))))

(deftest and-or-test
  (let [coll1 (ref nil)
        path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
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
             (sort-by :m (map #(dissoc % :id)
                              (db/find coll1
                                       (db/?and (db/?or {:m 4 :f 2})
                                                (db/?or {:e 5 :m 3})))))))
      (db/destroy! coll1))))
