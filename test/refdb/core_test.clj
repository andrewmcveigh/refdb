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
      (is (= {} (load-file (str (db/coll-file "coll1")))))
      (db/save! coll1 assoc-in [3 :test :test2 :thsteh] 2)
      (is (= {:test {:test2 {:thsteh 2}}} (db/get coll1 3))))))
