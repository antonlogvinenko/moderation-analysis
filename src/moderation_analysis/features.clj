(ns moderation-analysis.features
  (:require [clojure.data.json :as json])
  (:use [incanter core]))

(defn idx [coll idx1 idx2]
  (-> coll (nth idx1) (nth idx2)))

(defn n-ix-sum [n i]
  (+ (idx n i 0) (idx n i 1)))

(defn n-xi-sum [n i]
  (+ (idx n 0 i) (idx n 1 i)))

(defn mi-ij [n i j]
  (let [n-all (->> n
                   (map (partial reduce +))
                   (reduce +))
        nij (idx n i j)
        nix (n-ix-sum n i)
        nxj (n-xi-sum n j)]
    (if (zero? nij) 0
        (-> nij (* n-all) (/ nix) (/ nxj) log2 (* nij) (/ n-all)))))

(defn mi [n]
  (let [indexes [0 1]]
    (->> (for [i indexes, j indexes] [i j])
         (map #(apply mi-ij n %))
         (apply +))))