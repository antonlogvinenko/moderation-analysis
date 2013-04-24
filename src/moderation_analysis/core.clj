(ns moderation-analysis.core
   (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string]
            [clojure.data.json :as json])
  (:use [incanter core charts stats datasets]))


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
  
(defn count-moderation [history]
  (let [moderated-amount (->> history
                              (map :bulletin.adminPublishStatus)
                              (filter moder-pred)
                              count)]
    {moderated-amount 1}))

(def stat {:bulletinByEnqueueAmount
           count-moderation

           :bulletinByFirstEnqueueVersion
           (fn [h] (let [first-index (->> h
                                          (filter #(-> % :bulletin.adminPublishStatus moder-pred))
                                          first
                                          :bulletin.version)]
                     {first-index 1}))
           
           :bulletinByInitialAdminPublishStatus
           (fn [h] {(-> h first :bulletin.adminPublishStatus) 1})
           
           :bulletinByDirectory
           (fn [h] {(-> h first :bulletin.dir) 1})
           
           :enqueueByDirectory
           (fn [h] {(-> h first :bulletin.dir)
                    (->> h count-moderation keys first)})
           
           })

(defn get-history [row]
  (->> row :history json/read-json :ol (map :state)))

(defn sort-distr [distr]
  (->> distr
       (sort #(> (val %1) (val %2)))))

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

(defn row-analysis [{overall :overall time :time stat :stat} row]
  (let [row-stat (get-stat row)
        new-stat (merge-stat stat row-stat)
        overall (inc overall)
        now (System/currentTimeMillis)
        new-time (if (-> overall (rem 10000) zero?)
                   (do (-> now (- time) (/ 1000) double (println " sec - " overall))
                       now)
                   time)]
    {:overall overall :time new-time :stat new-stat}))

(def latest-request "select * from history2 where type='bulletin' order by user_space_id desc limit ?")

(def queue-request "select * from priority_moderation_queue as l left join history2 as r on l.bulletin_id = r.user_space_id where r.type='bulletin' limit ?")

(def all-bulletins "select * from history2 where type='bulletin' limit ?")

(defn analyze-hist [request file limit]
  (walk-rows mysql-history [request limit] rows
    (->> rows
         (pmap get-history)
         (reduce row-analysis {:overall 0 :time (System/currentTimeMillis) :stat {}})
         :stat
         sort-stat
         (spit file))))

(defn map-values [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn normalize-distribution [m]
  (let [overall (->> m vals (apply +))
        form (partial format "%.2f")]
    (map-values #(-> % (/ overall) (* 100) double form) m)))

(defn split-files []
  (let [file-names ["1", "5", "10", "all", "queue"]
        prefix "stats/dist-"
        dir-prefix "./stats/dir/"
        stat-prefix "./stats/stat/"
        dir-keys [:bulletinByDirectory, :enqueueByDirectory]
        stat-keys [:bulletinByFirstEnqueueVersion, :bulletinByEnqueueAmount, :bulletinByInitialAdminPublishStatus]
        maps (into {} (for [name file-names]
                        [name (->> name
                                   (str prefix)
                                   slurp
                                   read-string
                                   debug
                                   (map-values (comp (partial into {}) normalize-distribution))
                                   sort-stat)]))]
    (for [[name stats] maps]
      (do
        (spit (str dir-prefix name) (select-keys stats dir-keys))
        (spit (str stat-prefix name) (select-keys stats stat-keys))))))