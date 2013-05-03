(ns moderation-analysis.histdiff
  (:use [moderation-analysis mysql])
  (:require [clj-diff.core :as diff]
            [clojure.data.json :as json]))

(defn compute [{compressed :compressed original :original cnt :count} history]
  (let [history (->> history (filter (comp not nil?)))
        original-count (->> history
                            (map count)
                            (reduce +))
        compressed-count (->> history
                              (drop 1)
                              (interleave history)
                              (partition 2)
                              (map #(diff/diff (first %) (second %)))
                              (map str)
                              (map count)
                              (reduce +)
                              (+ (-> history first count)))]
    (if (-> cnt (rem 10000) zero?) (println cnt))
    {:count (inc cnt)
     :compressed (+ compressed compressed-count)
     :original (+ original original-count)}))

(defn show [f x] (-> x f println) x)

(defn compute [request limit]
  (walk-rows mysql-history [request limit] rows
    (->> rows
         (pmap (comp (partial map (comp :bulletin.text :state))
                     :ol
                     json/read-json
                     :history))
         (reduce compute {:compressed 0 :original 0 :count 0}))))