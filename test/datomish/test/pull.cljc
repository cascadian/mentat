(ns datomish.test.pull
  #?(:cljs
     (:require-macros
       [datomish.pair-chan :refer [go-pair <?]]
       [datomish.node-tempfile-macros :refer [with-tempfile]]
       [cljs.core.async.macros :as a :refer [go]]))
  (:require
    [datomish.api :as d]
    #?@(:clj
        [
    [datomish.pair-chan :refer [go-pair <?]]
    [datomish.jdbc-sqlite]
    [datomish.test-macros :refer [deftest-db]]
    [honeysql.core :as sql :refer [param]]
    [tempfile.core :refer [tempfile with-tempfile]]
    [clojure.test :as t :refer [is are deftest testing]]])
    #?@(:cljs
        [[datomish.js-sqlite]
         [datomish.test-macros :refer-macros [deftest-db]]
         [honeysql.core :as sql :refer-macros [param]]
         [datomish.node-tempfile :refer [tempfile]]
         [cljs.test :as t :refer-macros [is are deftest testing]]])
    [datomish.test.query :refer [<initialize-with-schema save-schema]])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(def schema
  [{:db/id                 (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality        :db.cardinality/one
    :db/valueType          :db.type/long
    :db/unique             :db.unique/identity
    :db/ident              :pull/id}
   {:db/id                 (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality        :db.cardinality/one
    :db/valueType          :db.type/string
    :db/ident              :pull/str}
   {:db/id                 (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality        :db.cardinality/many
    :db/valueType          :db.type/ref
    :db/ident              :pull/refs}
   {:db/id                 (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality        :db.cardinality/one
    :db/valueType          :db.type/ref
    :db/isComponent        true
    :db/ident              :pull/component}])

(deftest-db test-pull-api conn
            (let [attrs (<? (<initialize-with-schema conn schema))
                  tempid #(d/id-literal :db.part/user %)
                  tempid-1 (tempid -1)
                  tempid-2 (tempid -2)
                  tempid-3 (tempid -3)
                  tempid-4 (tempid -4)
                  tx-data (<? (d/<transact! conn
                                            [{:db/id    tempid-2
                                              :pull/id  2
                                              :pull/str "2"}
                                             {:db/id    tempid-3
                                              :pull/id  3
                                              :pull/str "3"}
                                             {:db/id    tempid-4
                                              :pull/id  4
                                              :pull/str "4"}
                                             {:db/id          tempid-1
                                              :pull/id        1
                                              :pull/str       "1"
                                              :pull/component tempid-2
                                              :pull/refs      [tempid-3
                                                               tempid-4]}]))
                  tempids (:tempids tx-data)
                  eid #(get tempids %)]
              (testing "pull wildcard"
                (is (= {:db/id (eid tempid-1)
                        :pull/id 1
                        :pull/str "1"
                        :pull/component {:db/id (eid tempid-2)
                                         :pull/id 2
                                         :pull/str "2"}
                        :pull/refs [{:db/id (eid tempid-3)}
                                    {:db/id (eid tempid-4)}]}
                       (<? (d/<pull (d/db conn) '[*] (eid tempid-1))))))
              (testing "pull reverse attribute"
                (is (= {:pull/_refs [{:pull/id 1 :db/id (eid tempid-1)}]}
                       (<? (d/<pull (d/db conn) '[{:pull/_refs [:pull/id :db/id]}] (eid tempid-4))))))
              (testing "pull with recursion"
                (is (= {:pull/id 1 :pull/refs [{:pull/id 3} {:pull/id 4}]}
                       (<? (d/<pull (d/db conn) '[:pull/id {:pull/refs ...}] (eid tempid-1))))))))

(deftest-db test-pull-in-find-spec conn
            (let [attrs (<? (<initialize-with-schema conn save-schema))
                  <q-one-with-find (fn [& args]
                                     (d/<q (d/db conn)
                                           `[:find ~@args
                                             :in ~'$
                                             :where
                                             [~'?save :save/excerpt "Some page excerpt"]]))
                  <q-many-with-find (fn [& args]
                                      (d/<q (d/db conn)
                                            `[:find ~@args
                                              :in ~'$
                                              :where
                                              ~'[?save :save/excerpt]]))
                  <q-with-find-no-results (fn [& args]
                                            (d/<q (d/db conn)
                                                  `[:find ~@args
                                                    :in ~'$
                                                    :where
                                                    ~'[?save :save/excerpt "NOT FOUND"]]))]
              (<? (d/<transact! conn
                                [{:db/id        (d/id-literal :db.part/user -1)
                                  :save/title   "Some page title"
                                  :save/excerpt "Some page excerpt"}
                                 {:db/id        (d/id-literal :db.part/user -2)
                                  :save/title   "A different page"
                                  :save/excerpt "A different excerpt"}]))
              (testing "pull with find relation spec"
                (let [find-spec '(pull ?save [:save/title :save/excerpt])
                      result (<? (<q-many-with-find find-spec))
                      empty-result (<? (<q-with-find-no-results find-spec))]
                  (is (= [] empty-result))
                  (is (= [[{:save/title   "Some page title"
                            :save/excerpt "Some page excerpt"}]
                          [{:save/title   "A different page"
                            :save/excerpt "A different excerpt"}]] result))))
              (testing "pull with find collection spec"
                (let [find-spec '[(pull ?save [:save/title]) ...]
                      result (<? (<q-many-with-find find-spec))
                      empty-result (<? (<q-with-find-no-results find-spec))]
                  (is (= [] empty-result))
                  (is (= [{:save/title "Some page title"}
                          {:save/title "A different page"}] result))))
              (testing "pull with find scalar spec"
                (let [find-spec ['(pull ?save [:save/title]) '.]
                      result (<? (apply <q-one-with-find find-spec))
                      empty-result (<? (apply <q-with-find-no-results find-spec))]
                  (is (= nil empty-result))
                  (is (= {:save/title "Some page title"} result))))
              (testing "pull with find tuple spec"
                (let [find-spec '[(pull ?save [:save/title])]
                      result (<? (<q-one-with-find find-spec))
                      empty-result (<? (<q-with-find-no-results find-spec))]
                  (is (= nil empty-result))
                  (is (= [{:save/title "Some page title"}] result))))))

