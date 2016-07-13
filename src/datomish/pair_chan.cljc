;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.pair-chan)

;; From https://github.com/plumatic/schema/blob/bf469889b730feb09448fd085be5828f28425b41/src/clj/schema/macros.clj#L10-L19.
(defn cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

(defmacro if-cljs
  "Return then if we are generating cljs code and else for Clojure code.
   https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
  [then else]
  (if (cljs-env? &env) then else))

;; It's a huge pain to declare cross-environment macros.  This is awful, but making the namespace a
;; parameter appears to be *even worse*.  Note also that `go` is not in a consistent namespace...
(defmacro go-pair [& body]
  "Evaluate `body` forms in a `go` block.  Catch errors and return a
  pair chan (a promise channel resolving to `[result error]`)."
  `(if-cljs
     (let [pc-chan# (cljs.core.async/promise-chan)]
       (cljs.core.async.macros/go
         (try
           (cljs.core.async/>! pc-chan# [(do ~@body) nil])
           (catch js/Error ex#
             (cljs.core.async/>! pc-chan# [nil ex#]))))
       pc-chan#)
     (let [pc-chan# (clojure.core.async/promise-chan)]
       (clojure.core.async/go
         (try
           (clojure.core.async/>! pc-chan# [(do ~@body) nil])
           (catch Exception ex#
             (clojure.core.async/>! pc-chan# [nil ex#]))))
       pc-chan#)))

;; Thanks to David Nolen for the name of this macro! http://swannodette.github.io/2013/08/31/asynchronous-error-handling/.
;; This version works a bit differently, though.  This must be a macro, so that the enclosed <!
;; symbols are processed by any enclosing go blocks.
(defmacro <?
  "Expects `pc-chan` to be a channel or ReadPort which produces [value nil] or
  [nil error] pairs, and returns values and throws errors as per `consume-pair`."
  [pc-chan]
  `(if-cljs
     (consume-pair (cljs.core.async/<! ~pc-chan))
     (consume-pair (clojure.core.async/<! ~pc-chan))))

(defn consume-pair
  "When passed a [value nil] pair, returns value. When passed a [nil error] pair,
  throws error. See also `<?`."
  [[val err]]
  (if err
    (throw err)
    val))
