(ns datomish.pull-query
  #?(:cljs
     (:require-macros
       [datomish.pair-chan :refer [go-pair <?]]
       [cljs.core.async.macros :refer [go]]))
  (:require
    [datomish.datom :as dd :refer [datom datom? #?@(:cljs [Datom])]]
    [datomish.util :as util
     #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]

    #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
    [clojure.core.async :as a :refer [chan go <! >!]]])
    #?@(:cljs [[datomish.pair-chan]
               [cljs.core.async :as a :refer [chan <! >!]]])
    [datascript.parser :as dp
     #?@(:cljs [:refer
                [FindRel FindColl FindTuple FindScalar
                 Pull]])])
  #?(:clj
     (:import
       [datomish.datom Datom]
       [datascript.parser
        FindRel FindColl FindTuple FindScalar
        Pull
        ])))

(defn pull-in-query
  [db pull-fn {:keys [elements find-spec]} res-chan]
  (if-let [pulls (seq (filter some? (map-indexed (fn [idx elem] (when (instance? Pull elem) [idx elem])) elements)))]
    (go-pair
      (let [res (<? res-chan)
            <pull (fn [pull-elem eid] (pull-fn db (get-in pull-elem [:pattern :value]) eid))]
        (cond
          (nil? res) res
          (instance? FindScalar find-spec)
          (<? (<pull (second (first pulls)) res))
          (empty? res) res
          (instance? FindColl find-spec)
          (let [pull-elem (second (first pulls))]
            (loop [query-result res
                   [eid & eids] res
                   count 0]
              (if eid
                (let [pull-result (<? (<pull pull-elem eid))]
                  (recur (assoc query-result count pull-result)
                         eids
                         (inc count)))
                query-result)))
          (instance? FindTuple find-spec)
          (loop [query-result (vec res)
                 [[idx pull-elem] & pulls] pulls]
            (if pull-elem
              (let [eid (nth res idx)
                    pull-result (<? (<pull pull-elem eid))]
                (recur (assoc query-result idx pull-result)
                       pulls))
              query-result))
          (instance? FindRel find-spec)
          (loop [query-result res
                 count 0
                 [tuple & tuples] res]
            (if tuple
              (let [query-result* (loop [tuple* (vec tuple)
                                         [[idx pull-elem] & pulls] pulls]
                                    (if pull-elem
                                      (let [eid (nth tuple idx)
                                            pull-result (<? (<pull pull-elem eid))]
                                        (recur (assoc tuple* idx pull-result) pulls))
                                      tuple*))]
                (recur (assoc query-result count query-result*) (inc count) tuples))
              query-result))
          :else res)))
    res-chan))
