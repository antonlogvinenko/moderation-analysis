(ns moderation-analysis.mutual-information
  (:use [moderation-analysis features mysql])
  (:require [clojure.data.json :as json])
  (:import [org.tartarus.snowball.ext RussianStemmer]
           [org.apache.lucene.analysis KeywordTokenizer LetterTokenizer]
           [org.apache.lucene.analysis.tokenattributes OffsetAttribute CharTermAttribute]))

(def stemmer (RussianStemmer.))

(defn get-stem [word]
  (doto stemmer (.setCurrent word) .stem)
  (.getCurrent stemmer))

(defn normalize-word [word]
  (-> word .trim .toLowerCase get-stem))

(defn to-words [text]
  (let [symbols "[0123456789.,:;?!-+%$@#^&*=±§<>'\"{}//°\\-_~`\"]"]
    (->> (.replaceAll text symbols " ")
         (.split #"\s+")
         vec
         (filter (comp not empty?))
         (map normalize-word))))

(defn restore-history [history r]
  (->> history
       (take (inc r))
       (reductions merge)
       (map :bulletin.text)
       (map #(if (nil? %) "" %))))

(defn get-added-words [[l r] history]
  (let [history (restore-history history r)
        left (apply hash-set (-> history (nth l) to-words distinct))
        right (apply hash-set (-> history (nth r) to-words distinct))
        difff (clojure.set/difference right left)]
    difff))

(defn sorted-words [key m]
  (sort #(> (second %1) (second %2))
        (filter (comp not nil?)
                (for [[word stat] m]
                  (let [value (stat key)]
                    (if (-> value nil? not)
                      [word value] nil))))))

(defn get-statuses [history]
  (map :bulletin.adminPublishStatus history))

(defn get-words [value history]
  (let [history (restore-history history value)
        words (apply hash-set (-> history (nth value) to-words distinct))]
    words))

(defn pluss [a]
  (if (nil? a) 1 (inc a)))

(defn get-text-versions [history]
  (let [statuses (get-statuses history)
        find-statuses (fn [status]
                        (->> statuses
                             (map-indexed #(if (= %2 status) %1 nil))
                             (filter (comp not nil?))))
        approves (find-statuses 1)
        declines (find-statuses -4)
        make-reduce (fn [k] (fn [stat value]
                              (reduce
                               (fn [m word]
                                 (update-in m [word k] pluss))
                               stat
                               (get-words value history))))
        words (reduce (make-reduce :bad)
                      (reduce (make-reduce :good) {} approves)
                      declines)]
    (assoc-in
     (assoc-in words [:info :bad] (count declines))
     [:info :good] (count approves))
    ))

(defn get-text-diffed [history]
  (let [statuses (get-statuses history)
        find-prev-version (fn [status x]
                            [(->> statuses
                                  (take x)
                                  reverse
                                  (drop-while #(or (nil? %) (= status %)))
                                  count
                                  dec)
                             x])
        select-clusters-fn (fn [status] (->> statuses
                                             (map-indexed #(if (= %2 status) %1 nil))
                                             (filter (comp not nil?))
                                             (filter pos?)
                                             (map (partial find-prev-version status))))
        approve-pairs (select-clusters-fn 1)
        decline-pairs (select-clusters-fn -4)
        make-reduce (fn [k] (fn [stat value]
                              (reduce
                               (fn [m word]
                                 (update-in m [word k] pluss))
                               stat
                               (get-added-words value history))))
        words (reduce (make-reduce :bad)
                      (reduce (make-reduce :good) {} approve-pairs)
                      decline-pairs)]
    (assoc-in
     (assoc-in words [:info :bad] (count decline-pairs))
     [:info :good] (count approve-pairs))))

;;{"word" {:good 10 :bad 10000} "another word" {:good 1 :bad 35}}


(defn zerofy [n]
  (if (nil? n) 0 n))

(defn create-n [good bad good-count bad-count]
  (let [good (zerofy good)
        bad (zerofy bad)
        n [[good bad]
           [(- good-count good) (- bad-count bad)]]]
    n))
  
(defn feature-selection [data]
  (let [good-count (->> data :info :good)
        bad-count (->> data :info :bad)]
    (for [[word {good :good bad :bad}] data]
      [word (mi (create-n good bad good-count bad-count))])))
           
(defn run [n]
  (letfn [(analyze [fun file]
            (->> fun (analyze-hist latest-request n) (spit file)))
          (features [file1 file2]
            (->> file1 slurp read-string feature-selection vec (spit file2)))
          (print-some [file]
            (->> file slurp read-string (sort #(> (second %1) (second %2))) (take 20) println))]
    (let [data-diffed "data-diffed", data-versions "data-versions"
          features-diffed "features-diffed", features-versions "features-versions"]
      (analyze get-text-diffed data-diffed)
      (analyze get-text-versions data-versions)
      (features data-diffed features-diffed)
      (features data-versions features-versions)
      (print-some features-diffed)
      (print-some features-versions))))        