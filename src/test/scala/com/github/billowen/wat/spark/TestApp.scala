package com.github.billowen.wat.spark

import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object TestApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-load-wat")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    LoadTestData.load("dev", "wafer.csv", sc)

    sc.stop()
  }
}
