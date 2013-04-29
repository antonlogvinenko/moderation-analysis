(defproject moderation-analysis "1.0.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [
                 [incanter "1.3.0"]
                 [org.clojure/clojure "1.3.0"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [org.clojure/data.json "0.2.2"]
                 [org.apache.lucene/lucene-analyzers "3.6.0"]
                 [org.apache.lucene/lucene-core "3.6.0"]
                 [clj-diff "1.0.0-SNAPSHOT"]
                 ]
  :jvm-opts ["-Xmx1024M" "-server"])