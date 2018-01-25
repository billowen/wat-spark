import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
.setMaster("local")
.setAppName("test")
.set("spark.cassandra.connection.host", "127.0.0.1")

val sc = new SparkContext(conf)

