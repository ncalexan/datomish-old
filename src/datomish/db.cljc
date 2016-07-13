;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  #?(:clj
     (:require
      [datomish.sqlite :as s]
      [datomish.sqlite-schema :as sqlite-schema]
      [datomish.pair-chan :refer [go-pair <?]]
      [clojure.core.async :refer [go <! >!]])
     :cljs
     (:require
      [datomish.sqlite :as s]
      [datomish.sqlite-schema :as sqlite-schema]
      [datomish.pair-chan]
      [cljs.core.async :as a :refer [<! >!]])))

(defprotocol IDB
  (close
    [db]
    "Close this database. Returns a pair channel of [nil error]."))

(defrecord DB [sqlite-connection]
  IDB
  (close [db] (close (.-sqlite-connection db))))

(declare ensure-current-version)

(defn with-sqlite-connection [sqlite-connection]
  (go-pair
    (when-not (= sqlite-schema/current-version (<? (sqlite-schema/ensure-current-version sqlite-connection)))
      (throw (Exception. "badness ensued"))) ;; TODO: raise
    (->DB [sqlite-connection])))
