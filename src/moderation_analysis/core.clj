(ns moderation-analysis.core
  (:require [clojure.java.jdbc :as sql]))

(def mysql-properties {:classname "com.mysql.jdbc.Driver"
                       :subprotocol "mysql"
                       :subname "//127.0.0.1:3306/test"
                       :user ""
                       :password ""})

(defmacro with-connection [query results & body]
  `(sql/with-connection mysql-properties
      (sql/with-query-results ~results ~query ~@body)))

(defmacro walk-rows [query results & body]
  `(with-connection
       (->> ~query
            (cons {:fetch-size Integer/MIN_VALUE
                   :prefetch-size 10000
                   :chunk-size 10000})
            vec)
       ~results ~@body))

(defn get-decisions [status user]
  (with-connection ["select count(*) as c from bulletin_moderation_history where bulletin_owner_id = ? and user_id != 0 and status = ?" user status]
      result (-> result first :c)))

(defn get-rating [user]
  (/ (get-decisions "declined" user) (get-decisions "approved" user)))

(defn save [ratings]
  (doseq [rating ratings]
    (println rating)))
  
(defn run []
  (walk-rows
      ["select distinct bulletin_owner_id from bulletin_moderation_history limit 100"]
      users
    (->> users (map :bulletin_owner_id) (map get-rating) save)))
    
    