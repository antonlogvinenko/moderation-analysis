(ns moderation-analysis.mysql
  (:require [clojure.java.jdbc :as sql]
            [clojure.data.json :as json]))


(def mysql-properties {:classname "com.mysql.jdbc.Driver"
                       :subprotocol "mysql"
                       :subname "//127.0.0.1:3306/test"
                       :user ""
                       :password ""})

(def mysql-history {:classname "com.mysql.jdbc.Driver"
                    :subprotocol "mysql"
                    :subname "//127.0.0.1:3306/bul_history"
                    :user "anton"
                    :password ""})

(defmacro with-connection [props query results & body]
  `(sql/with-connection ~props
      (sql/with-query-results ~results ~query ~@body)))

(def lazy
  {:fetch-size Integer/MIN_VALUE
   :concurrency :read-only
   :result-type :forward-only
   :prefetch-size 1000
   :chunk-size 1000})

(defmacro walk-rows [props query results & body]
  `(with-connection ~props
       (->> ~query (cons lazy) vec)
       ~results ~@body))

(defn merge-stat [stat new-stat]
  (merge-with (partial merge-with +) stat new-stat))

(defn get-history [row]
  (->> row :history json/read-json :ol (map :state)))

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


(defn analyze-hist [request limit reduce-fun]
  (walk-rows mysql-history [request limit] rows
    (->> rows
         (pmap get-history)
         (reduce (create-reduce reduce-fun) {:overall 0 :time (System/currentTimeMillis) :stat {}})
         :stat)))

