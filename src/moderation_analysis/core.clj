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


(defn first-moderation [history]
  (let [first-index (->> history
                         (filter #(-> % :bulletin.adminPublishStatus moder-pred))
                         first
                         :bulletin.version)]
    {first-index 1}))

(defn dir-moderation [history]
  {(-> history first :bulletin.dir)
   (->> history count-moderation keys first)})

(defn bull-distr [history]
  {(-> history first :bulletin.dir) 1})

(defn status-distr [history]
  {(-> history first :bulletin.adminPublishStatus) 1})

(defn fake-distr [history] (println history))

(def stat {:adminStatusDistribution
           (fn [h]
             {(-> h first :bulletin.adminPublishStatus) 1})

           :bulltinsDistribution
           (fn [h]
             {(-> h first :bulletin.dir) 1})
           
           })

(defn run-stat [])

(defn sort-distr [distr]
  (->> distr
       (sort #(> (val %1) (val %2)))))

(defn create-analyze-hist [get-stat-fn]
  (fn [stat row]
    (->> row
         get-stat-fn
         (merge-with + stat))))

(defn get-history [row]
  (->> row :history json/read-json :ol (map :state)))

(defn analyze-history [limit stat-fn]
  (walk-rows
      mysql-history
      ["select * from history2 where type='bulletin' order by user_space_id desc limit ?" limit]
      rows
    (->> rows
         (take 2)
         (pmap get-history)
         (reduce (create-analyze-hist stat-fn) {})
         sort-distr)))

(defn aaa [{overall :overall time :time} y]
  (if (-> overall (rem 1000000) zero?)
    (let [now (System/currentTimeMillis)]
      (do (-> now (- time) (/ 1000) double (println overall " - "))
          {:overall (inc overall) :time now}))
    {:overall (inc overall) :time time}))


(defn mysql-slow [limit]
  (walk-rows
      mysql-history
      ["select * from history2 limit ?" limit]
      rows
    (->> rows (reduce aaa {:overall 0 :time (System/currentTimeMillis)}))))
         