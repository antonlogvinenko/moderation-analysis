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

(def stat {:initialAdminPublishStatusByBulletin
           (fn [h] {(-> h first :bulletin.adminPublishStatus) 1})

           :directoriyByBulletin
           (fn [h] {(-> h first :bulletin.dir) 1})

           :directoriyByEnqueue
           (fn [h] {(-> h first :bulletin.dir)
                    (->> h count-moderation keys first)})

           :firstEnqueueVersionByBulletin
           (fn [h] (let [first-index (->> h
                                          (filter #(-> % :bulletin.adminPublishStatus moder-pred))
                                          first
                                          :bulletin.version)]
                     {first-index 1}))
             
           :enqueueAmountByBulletin
           count-moderation         
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
  (into {}
        (for [[k v] new-stat]
          [k (merge-with + v (k new-stat))])))

(defn get-stat [row]
  (into {}
        (for [[name fun] stat]
          [name (fun stat)])))
    
(defn row-analysis [{overall :overall time :time stat :stat} row]
  (let [row-stat (get-stat row)
        new-stat (merge-stat stat row-stat)
        overall (inc overall)
        now (System/currentTimeMillis)
        new-time (if (-> overall (rem 100000) zero?)
                   (do (-> now (- time) (/ 1000) double (println " sec - " overall))
                       now)
                   time)]
    {:overall overall :time new-time :stat new-stat}))

(def sql-request "select * from history2 limit ?")

(defn analyze-hist [request file limit]
  (walk-rows mysql-history [request limit] rows
    (->> rows
         (take 2)
         (pmap get-history)
         (reduce row-analysis {:overall 0 :time (System/currentTimeMillis) :stat {}})
         :stat
         sort-stat
         (spit file))))         
         