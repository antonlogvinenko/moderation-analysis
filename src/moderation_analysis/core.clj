(ns moderation-analysis.core
   (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string])
   (:use [incanter core charts stats datasets]
         [moderation-analysis features mysql]))


(defn debug [x] (println x) x)

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


(defn sort-distr [distr]
  (->> distr
       (sort #(> (val %1) (val %2)))
       (into {})))

(defn sort-stat [stat]
  (into {}
        (for [[k v] stat]
          [k (sort-distr v)])))

(defn get-stat [row]
  (into {}
        (for [[name fun] stat]
          [name (fun row)])))

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