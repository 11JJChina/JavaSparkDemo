./spark-2.3.2-bin-hadoop2.7/bin/spark-submit \
--master spark://192.168.119.163:7077 \
--class com.bolingcavalry.sparkwordcount.WordCount \
--executor-memory 512m \
--total-executor-cores 2 \
~/jars/sparkwordcount-1.0-SNAPSHOT.jar \
192.168.119.163 \
8020 \
GoneWiththeWind.txt
