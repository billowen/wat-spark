package com.github.billowen.wat.spark

import org.apache.spark.{SparkConf, SparkContext}

object TestApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-load-wat")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    LoadDuts.load("demo", "duts.csv", sc)
    sc.stop()
  }
}
