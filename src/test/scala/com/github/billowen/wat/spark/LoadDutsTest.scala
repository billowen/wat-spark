package com.github.billowen.wat.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec
import org.scalatest._

class LoadDutsTest extends FlatSpec with BeforeAndAfter {
  var sc:SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-load-wat")
    sc = new SparkContext(conf)
  }
  after {
    sc.stop()
  }

  "Convert data into RDD[Dut]" should "successful in text" in {
    val headers = List("cellName", "module", "designType", "L", "mosType")
    val data = Array("Core_N_001, MisMatch, Core_N, 0.016, N")
    val expect = Dut("Core_N_001", "MisMatch", "Core_N", Map("L"->"0.016", "mosType"->"N"))
    val strRdd = sc.parallelize(data)
    val actual = LoadDuts.load(headers, strRdd)
    assert(actual.first() == expect)
  }
}