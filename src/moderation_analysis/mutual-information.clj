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

(defn get-stat [m k]
  (if-let [v (k m)] v 1))



;;searching for most frequent words in approves/declines
(defn check-reduce [acc v]
  (let [info (second v)
        k (first v)
        amount (count acc)]
    (if (-> amount (mod 100000) zero?) (println amount))
    (assoc acc k
           (assoc info
           :rate (double (/ (get-stat info :good) (get-stat info :bad)))))))    

(defn sort-check [comparison]
  (->> "output"
       slurp
       read-string
       (sort #(comparison (-> %2 second :rate) (-> %1 second :rate)))
       (take 20)
       vec
       (spit "output-3")))

(defn check []
  (let [data (->> "data-versions" slurp read-string)
        amount (count data)]
    (println "Overall: " amount)
    (->> data
         (reduce check-reduce {})
         (spit "output"))))


(def typed-dirs [83 106 332 775 773 338 340 349 393 461 503 171 604 337 594 615 692 772 240 335 616 352 774 125 434 403 241 3])
;;BZR-10750
(defn untyped-bulletins [history]
  (let [bulletin (->> history (reductions merge) last)
        dir (-> bulletin :bulletin.dir)
        type (:bulletin.type bulletin)
        lemma (:type.lemma bulletin)
        counts? (and (nil? lemma)
                     (not= type "bulletinAdvertisement")
                     (some (partial = dir) typed-dirs))]
;;    (if (nil? lemma) nil (println lemma))
    {:untyped {:count (if counts? 1 0)}}
    ))



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