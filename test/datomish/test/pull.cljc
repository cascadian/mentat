(ns datomish.test.pull
  #?(:cljs
     (:require-macros
       [datomish.pair-chan :refer [go-pair <?]]
       [datomish.node-tempfile-macros :refer [with-tempfile]]
       [cljs.core.async.macros :as a :refer [go]]))
  (:require
    [datomish.api :as d]
    #?@(:clj
        [[datomish.pair-chan :refer [go-pair <?]]
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
                      [{:db/id (d/id-literal :db.part/user -1)
                        :save/title "Some page title"
                        :save/excerpt "Some page excerpt"}
                       {:db/id (d/id-literal :db.part/user -2)
                        :save/title "A different page"
                        :save/excerpt "A different excerpt"}]))
    (testing "pull with find relation spec"
      (let [find-spec '(pull ?save [:save/title :save/excerpt])
            result (<? (<q-many-with-find find-spec))
            empty-result (<? (<q-with-find-no-results find-spec))]
        (is (= [] empty-result))
        (is (= [[{:save/title "Some page title"
                  :save/excerpt "Some page excerpt"}]
                [{:save/title "A different page"
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

