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


(def mysql-history {:classname "com.mysql.jdbc.Driver"
                    :subprotocol "mysql"
                    :subname "//127.0.0.1:3307/bul_history"
                    :user "bul_history"
                    :password (slurp "pswd.txt")})

(defn get-history []
  (walk-rows mysql-history
      ["select * from history2 limit 1"]
      rows
    (-> rows first :history json/read-json)))