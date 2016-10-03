;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go go-loop]]))
  (:require
   [datomish.api :as d]
   [datomish.db.debug :refer [<datoms-after <datoms>= <transactions-after <shallow-entity <fulltext-values]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.schema :as ds]
   [datomish.simple-schema]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema]
   [datomish.datom]
   #?@(:clj [[datomish.jdbc-sqlite]
             [datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async deftest-db]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :as a :refer [go go-loop <! >!]]])
   #?@(:cljs [[datomish.js-sqlite]
              [datomish.pair-chan]
              [datomish.test-macros :refer-macros [deftest-async deftest-db]]
              [datomish.node-tempfile :refer [tempfile]]
              [cljs.test :as t :refer-macros [is are deftest testing async]]
              [cljs.core.async :as a :refer [<! >!]]]))
  #?(:clj
     (:import [clojure.lang ExceptionInfo]))
  #?(:clj
     (:import [datascript.db DB])))

#?(:cljs
   (def Throwable js/Error))

(defn- tempids [tx]
  (into {} (map (juxt (comp :idx first) second) (:tempids tx))))

(def test-schema
  [{:db/id        (d/id-literal :db.part/user)
    :db/ident     :x
    :db/unique    :db.unique/identity
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :name
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :y
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :age
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :email
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :spouse
    :db/unique    :db.unique/value
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :friends
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref
    :db.install/_attribute :db.part/db}
   ])

(deftest-db test-listeners conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))

        rs1 (atom [])
        l1  (partial swap! rs1 conj)
        rs2 (atom [])
        l2  (partial swap! rs2 conj)]

    (testing "no listeners is okay"
      ;; So that we can upsert to concrete entids.
      (<? (d/<transact! conn [[:db/add 101 :name "Ivan"]
                              [:db/add 102 :name "Petr"]])))


    (testing "listeners are added, not accidentally notified of events before they were added"
      (is (= "key1" (d/listen! conn "key1" l1)))
      (is (= "key2" (d/listen! conn "key2" l2)))
      (is (= 0
             (count @rs1)))
      (is (= 0
             (count @rs2))))

    (testing "unlistening to unrecognized key is ignored"
      (d/unlisten! conn "UNKNOWN KEY"))

    (testing "listeners observe reports"
      (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :name "Ivan"]]))
      (is (= 1
             (count @rs1)))
      (is (= {-1 101}
             (tempids (first @rs1))))
      (is (= 1
             (count @rs2)))
      (is (= {-1 101}
             (tempids (first @rs2)))))

    (testing "unlisten removes correct listener"
      (d/unlisten! conn "key1")

      (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -2) :name "Petr"]]))
      (is (= 1
             (count @rs1)))
      (is (= 2
             (count @rs2)))
      (is (= {-2 102}
             (tempids (second @rs2)))))

    (testing "returning to no listeners is okay"
      (d/unlisten! conn "key2")

      (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :name "Petr"]]))
      (is (= 1
             (count @rs1)))
      (is (= 2
             (count @rs2))))

    (testing "complains about blocking channels"
      (is (thrown-with-msg?
            ExceptionInfo #"unblocking buffers"
            (d/listen-chan! conn (a/chan 1)))))
    ))

#_ (time (t/run-tests))
