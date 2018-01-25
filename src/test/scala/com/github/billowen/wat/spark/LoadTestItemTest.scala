package com.github.billowen.wat.spark

import java.io.FileNotFoundException
import java.util.UUID

import com.github.billowen.wat.spark.LoadTestItems.load
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class LoadTestItemTest extends FlatSpec with BeforeAndAfter {
  var sc:SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test-load-wat")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    sc = new SparkContext(conf)
  }
  after {
    sc.stop()
  }

  "Create test item" should "successful" in {
    val headers = List("item", "unit", "measurementType", "structureName",
      "formula", "module", "designType", "target", "specLow", "specHigh", "controlLow", "controlHigh")
    val data = List("logmm_Par_Core_N_001_Idl", "", "Idl", "logmm_Par_Core_N_001", "$value==-1?1:$value*1000000.0/2.0",
      "MisMatch", "logmm_Par_Core_N", "", "", "", "", "")
    val projectId = UUID.randomUUID()
    val testId = UUID.randomUUID()
    val dutId = UUID.randomUUID()
    val dutMap = Map("logmm_Par_Core_N_001" -> dutId)
    val expect = TestItem(projectId, "logmm_Par_Core_N_001_Idl")
    expect.unit = data(1)
    expect.measurement = data(2)
    expect.dut_name = "logmm_Par_Core_N_001"
    expect.formula = data(4)
    val actual = LoadTestItems.createTestItem(headers, data, projectId)

    assert(actual == expect)
  }

  "Invalid data" should "not create rdd" in {
    val headers = List("item", "unit", "measurementType", "structureName",
      "formula", "module", "designType", "target", "specLow", "specHigh", "controlLow", "controlHigh")
    val data = List(", , Idl, logmm_Par_Core_N_001, $value==-1?1:$value*1000000.0/2.0, MisMatch, logmm_Par_Core_N, , , , , ")
    val projectId = UUID.randomUUID()
    val dataRdd = sc.parallelize(data)
    val actual = LoadTestItems.convert(headers, dataRdd, projectId)
    assert(actual.count == 0)
  }

}
