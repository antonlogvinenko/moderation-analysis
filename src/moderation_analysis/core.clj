(ns moderation-analysis.core
   (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string]
            [clojure.data.json :as json])
   (:use [incanter core charts stats datasets]
         [moderation-analysis features])
   (:import [org.tartarus.snowball.ext RussianStemmer]
            [org.apache.lucene.analysis KeywordTokenizer LetterTokenizer]
            [org.apache.lucene.analysis.tokenattributes OffsetAttribute CharTermAttribute]))



(def mysql-properties {:classname "com.mysql.jdbc.Driver"
                       :subprotocol "mysql"
                       :subname "//127.0.0.1:3306/test"
                       :user ""
                       :password ""})

(defmacro with-connection [props query results & body]
  `(sql/with-connection ~props
      (sql/with-query-results ~results ~query ~@body)))

(def lazy
  {:fetch-size Integer/MIN_VALUE
   :concurrency :read-only
   :result-type :forward-only
   :prefetch-size 10000
   :chunk-size 10000})

(defmacro walk-rows [props query results & body]
  `(with-connection ~props
       (->> ~query (cons lazy) vec)
       ~results ~@body))

(defn get-decisions [status user]
  (with-connection mysql-properties ["select count(*) as c from bulletin_moderation_history where bulletin_owner_id = ? and user_id != 0 and status = ?" user status]
      result (-> result first :c)))

(defn get-rating [user]
  (let [a (get-decisions "approved" user)
        d (get-decisions "declined" user)
        sum (+ a d)]
    (if (zero? sum) 0 (/ a sum))))

;720456
(def c 720456)

(defn save-data [ratings]
  (doseq [rating ratings]
    (spit "ratings.txt"
          (str rating " ")
          :append true)))
  
(defn run []
  (walk-rows mysql-properties ["select distinct bulletin_owner_id from bulletin_moderation_history limit ?" c] users
    (->> users
         (map :bulletin_owner_id)
         (map get-rating)
         save-data)))


(def default-width 1100)
(def default-height 440)

(defn show [n]
  (let [ratings-str (-> "ratings.txt" slurp (string/split #" "))
        ratings (->> ratings-str (map read-string) (take n))]
    (-> ratings
        (histogram :x-label "rating" :nbins 30)
        (view :width default-width :height default-height))))



;; Analyzing moderation bulletin history

(defn debug [x] (println x) x)

(def mysql-history {:classname "com.mysql.jdbc.Driver"
                    :subprotocol "mysql"
                    :subname "//127.0.0.1:3306/bul_history"
                    :user "anton"
                    :password ""})

(defn count-versions [history]
  (count history))

(defn moder-pred [status]
  (or (= status 0) (= status -1)))
  
(defn count-enqueues [history]
  (let [enqueues (->> history
                      (map :bulletin.adminPublishStatus)
                      (filter (comp not nil?))
                      count)]
    {enqueues 1}))

(def stat-0
  {:bulletinByDirectory
   (fn [h] {(-> h first :bulletin.dir) 1})
   
   :enqueueByDirectory
   (fn [h] {(-> h first :bulletin.dir)
            (->> h count-enqueues keys first)})})

(def stat {:bulletinByEnqueueAmount
           count-enqueues
           
           :bulletinByFirstEnqueueVersion
           (fn [h] (let [first-index (->> h
                                          (filter #(-> % :bulletin.adminPublishStatus nil? not))
                                          first
                                          :bulletin.version)]
                     {first-index 1}))
           
           :bulletinByInitialAdminPublishStatus
           (fn [h] {(-> h first :bulletin.adminPublishStatus) 1})
           
           :bulletinByDirectory
           (fn [h] {(-> h first :bulletin.dir) 1})
           
           :enqueueByDirectory
           (fn [h] {(-> h first :bulletin.dir)
                    (->> h count-enqueues keys first)
            })

           :ignoredModerations
           (fn [h] (let [count (-> h count-enqueues keys first)
                         initial (-> h first :bulletin.adminPublishStatus)
                         ignored (or false (and (= 1 count) (= 0 initial)))]
                     {:ignored (if ignored 1 0)}))

           })

(def stat-1
  {
   :ignoredModerations
   (fn [h] (let [count (-> h count-enqueues keys first)
                 initial (-> h first :bulletin.adminPublishStatus)
                 ignored (or false (and (= 1 count) (= 0 initial)))]
             {:ignored (if ignored 1 0)}))
   })

(defn get-history [row]
  (->> row :history json/read-json :ol (map :state)))

(defn sort-distr [distr]
  (->> distr
       (sort #(> (val %1) (val %2)))
       (into {})))

(defn sort-stat [stat]
  (into {}
        (for [[k v] stat]
          [k (sort-distr v)])))

(defn merge-stat [stat new-stat]
  (merge-with (partial merge-with +) stat new-stat))

(defn get-stat [row]
  (into {}
        (for [[name fun] stat]
          [name (fun row)])))

(defn create-reduce [reduce-fun]
  (fn [{overall :overall time :time stat :stat} row]
    (let [row-stat (reduce-fun row)
          new-stat (merge-stat stat row-stat)
          overall (inc overall)
          now (System/currentTimeMillis)
          new-time (if (-> overall (rem 10000) zero?)
                     (do (-> now (- time) (/ 1000) double (println " sec - " overall))
                         now)
                     time)]
      {:overall overall :time new-time :stat new-stat})))

(def latest-request "select * from history2 where type='bulletin' order by user_space_id desc limit ?")

(def queue-request "select * from priority_moderation_queue as l left join history2 as r on l.bulletin_id = r.user_space_id where r.type='bulletin' limit ?")

(def all-bulletins "select * from history2 where type='bulletin' limit ?")






(def stemmer (RussianStemmer.))

(defn remove-shit [text]
  (let [symbols "[0123456789.,:;?!-+%$@#^&*=±§<>'\"{}//°\\-_~`\"\\\\]"]
    (-> text
        (.replaceAll symbols "")
        (.replaceAll "\\\\" ""))))

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
           
        
  
  



(defn analyze-hist [request limit reduce-fun]
  (walk-rows mysql-history [request limit] rows
    (->> rows
         (pmap get-history)
         (reduce (create-reduce reduce-fun) {:overall 0 :time (System/currentTimeMillis) :stat {}})
         :stat)))


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



(defn map-values [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn invert-values [f m]
  (into (sorted-map) (for [[k v] m] [(f v) k])))

(defn normalize-distribution [m]
  (let [overall (->> m vals (apply +))
        form (partial format "%.2f")]
    (invert-values #(-> % (/ overall) (* 100) double form read-string) m)))

(defn split-files []
  (let [file-names ["5", "queue"]
        prefix "stats/dist-"
        dir-prefix "./stats/dir/"
        stat-prefix "./stats/stat/"
        dir-keys [:bulletinByDirectory, :enqueueByDirectory]
        stat-keys [:ignoredModerations :bulletinByFirstEnqueueVersion, :bulletinByEnqueueAmount, :bulletinByInitialAdminPublishStatus]
        maps (into {} (for [name file-names]
                        [name (->> name
                                   (str prefix)
                                   slurp
                                   read-string
                                   (map-values (partial into {}))
                                   (map-values normalize-distribution)
                                   )]))]
    (for [[name stats] maps]
      (do
        (spit (str dir-prefix name) (select-keys stats dir-keys))
        (spit (str stat-prefix name) (select-keys stats stat-keys))))))