from pyspark_cassandra import CassandraSparkContext, Row
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("PySpark Cassandra Test").set("spark.cassandra.connection.host", "127.0.0.1")
sc = CassandraSparkContext(conf=conf)
data = sc.cassandraTable("mykeyspace", "user",row_format = 1).collect()
rdd = sc.parallelize(data)
print (rdd.collect())
