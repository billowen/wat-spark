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
    val headers = Array("item", "unit", "measurementType", "structureName",
      "formula", "module", "designType", "target", "specLow", "specHigh", "controlLow", "controlHigh")
    val data = Array("logmm_Par_Core_N_001_Idl", "", "Idl", "logmm_Par_Core_N_001", "$value==-1?1:$value*1000000.0/2.0",
      "MisMatch", "logmm_Par_Core_N", "", "", "", "", "")
    val projectId = UUID.randomUUID()
    val testId = UUID.randomUUID()
    val dutId = UUID.randomUUID()
    val dutMap = Map("logmm_Par_Core_N_001" -> dutId)
    val expect = TestItem(projectId)
    expect.name = data(0)
    expect.unit = data(1)
    expect.measure_type = data(2)
    expect.dut_id = Some(dutId)
    expect.formula = data(4)
    val strRdd = sc.parallelize(data)
    val actual = LoadTestItems.createTestItem(headers, data, projectId, dutMap)

    // Set the test id to the same
    actual.test_id = testId
    expect.test_id = testId
    assert(actual == expect)
  }

  "Load a test item sample data to database" should "successful" in {
    val fileName = "sample_items.csv"
    val projectName = "demo"
    println(fileName)
    try {
      val error = load(projectName, fileName, sc)
    } catch {
      case ex : FileNotFoundException => println(s"File $fileName not found")
    }
  }

}
