(ns refdb.core-test
  (:require
    [clojure.test :refer :all]
    [clojure.set :as set]
    [refdb.core :as db]
    [riddley.walk :as walk]))

(def coll1 (ref nil))

(deftest all-test
  (let [;coll1 (ref nil)
        path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
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
  (let [;coll1 (ref nil)
        path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
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
  (let [;coll1 (ref nil)
        path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]
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

;; (defn doc-forms [doc special-forms & forms]
;;   (doall
;;    (map (partial walk/walk-exprs
;;                  #(and (sequential? %) (some (partial = (first %)) doc))
;;                  #(when %
;;                     (apply list 'list
;;                            (list 'quote (first %))
;;                            (apply doc-forms doc special-forms (rest %))))
;;                  (set (concat doc special-forms)))
;;         forms))
;; )

(defn doc-forms [special-forms & forms]
  (let [test-forms (set (keys (ns-map 'clojure.test)))
        special-forms (set (concat special-forms test-forms))]
    (doall
     (map (partial walk/walk-exprs
                   #(and (sequential? %)
                         (some (partial = (first %)) special-forms))
                   #(when (and % (sequential? %))
                      (when (meta %) (prn (meta %)))
                      (cond (:example (meta %))
                            ;; (some (partial = (first %))
                            ;;          (set/difference special-forms test-forms))
                            [(apply list 'list
                                    (list 'quote (first %))
                                    (apply doc-forms special-forms (rest %)))
                             (apply list (apply doc-forms special-forms %))]
                            :else
                            (apply list (apply doc-forms special-forms %))))
                   special-forms)
          forms)))
)

(defmacro with-examples
  [name & body]
  (apply doc-forms
         #{'db/init!
           'db/save!
           'db/with-refdb-path
           'db/with-transaction
           'db/transaction
           'db/find
           'db/history
           'db/previous}
         body))

;; (clojure.pprint/pprint
;;  (macroexpand
;;   '(with-examples doc-demo-test

;;      (let [path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))]

;;        (dosync (ref-set coll1 nil))

;;        ^{:example true
;;          :doc "Bind `refdb.core/*path*` within the context of this form."}
;;        (db/with-refdb-path path

;;          ^{:example true
;;            :doc "The collection should be initialized before use, but
;;               within the context of `refdb.core/*path*` being bound."}
;;          (db/init! coll1)

;;          (let [{id :id :as saved} (first (db/save! coll1 {:m 1}))]

;;            ^:example
;;            (db/save! coll1 (assoc saved :a 2))

;;            ^:example
;;            (db/save! coll1 (assoc saved :b 3))

;;            (is (= [{:id id :m 1 :a 2} {:id id :m 1}]
;;                   (map #(dissoc % :inst)
;;                        (db/history coll1 saved))))
;;            (is (= {:m 1 :a 2 :id id}
;;                   (dissoc (db/previous coll1 saved) :inst)))
;;            (dosync (ref-set coll1 nil))
;;            (db/init! coll1)
;;            (is (= [{:id id :m 1 :a 2} {:id id :m 1}]
;;                   (map #(dissoc % :inst)
;;                        (db/history coll1 (first (db/find coll1 nil))))))
;;            (db/with-transaction ^:test123 'testtrans
;;              (db/save! coll1 {:ttt 33 :ff 88}))
;;            (dosync (ref-set coll1 nil))
;;            (db/init! coll1)
;;            (let [trns (db/transaction (first (db/find coll1 :ttt 33)))]
;;              (is (= "testtrans" (:name trns)))
;;              (is (= {:test123 true} (:meta trns)))))
;;         (db/destroy! coll1)


;; (clojure.pprint/pprint
;;  (macroexpand
;;   '(with-examples readme

;;      ;;; A ref is needed, per collection, to store the `database`.
;;      (def employee (ref nil))

;;      (def account (ref nil))

;;      ;;; Bind `refdb.core/*path*` within the context of this form.
;;      (db/with-refdb-path (str "/tmp/refdb-" (+ 5000 (int (rand 100000))))

;;        ;;; The collection should be initialized before use, but within
;;        ;;; the context of `refdb.core/*path*` being bound.
;;        (db/init! employee)

;;        (db/init! account)

;;        ;;; Save an item in the collection. The result of `db/save!` is
;;        ;;; returned. Operations that `alter` the `collection` must be
;;        ;;; wrapped a transaction, however `db/save!` wraps it's own
;;        ;;; transaction if not already wrapped by one. A transaction
;;        ;;; returns a list of results, as it can contain more than one
;;        ;;; operation.
;;        (let [saved-1st (db/save! employee {:first-name "Jeff"})]

;;          ;;; Re-save the item with a keyval `assoc`ed.
;;          (db/save! employee (assoc saved-1st :last-name "Horrible"))

;;          ;;; Re-save the item with a keyval `assoc`ed.
;;          (db/save! employee
;;                    (assoc saved-1st :dob #inst "2078-11-03T00:00:00.000-00:00"))

;;          ;;; Transactions can be named, and metadata can be
;;          ;;; saved. They may contain one or more body
;;          ;;; expressions. Each expression *must* evaluate to satisfy
;;          ;;; `map?`. #{db/save! db/update} satisfy this condition.
;;          (db/with-transaction ^{:action :retirement} retire

;;            (db/save! employee (assoc saved-1st :retired true :active nil))

;;            (let [[{acc-no :id}] (db/save! account
;;                                           {:emyloyee (:id saved-lst)
;;                                            :hush-money 1e7})])

;;            (db/update! account acc-no assoc :status :closed)

;;            ;;; Custom body expressions may also be used. Keyvals other
;;            ;;; than #{:pre :sync :post} will be ignored. The result of
;;            ;;; :sync will be returned by the transaction.
;;            {:sync (alter account update-in [:items acc-no] conj nil)
;;             :post (doseq [f (reverse
;;                              (file-seq (io/file (:id saved-1st))))]
;;                     (.delete x))})
;;          )



;;        )
;;      )))
