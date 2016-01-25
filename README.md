##zx.soft.apt.spark

###KafkaToSpark
spark连接kafka，统计消耗数据量;
注意当运行方式为
local，与spark启动与否没有关系;
spark:// 这是用到了Spark的Standalone模式;此时工程的打包方式注意把依赖包打进去，否则在运行时则会报与spark-streaming-kafka_2.10包相关类ClassNotFound错误.

运行方式：
./bin/spark-submit apt-0.0.1-SNAPSHOT-jar-with-dependencies.jar --class 'zx.soft.apt.spark.KafkaToSpark' --master spark://Company:7077

