package com.github.billowen.wat.spark

import java.util.UUID

import org.scalatest.{BeforeAndAfter, FlatSpec}

class LoadTestDataTest extends FlatSpec with BeforeAndAfter {

  "Create one test data" should "success" in {
    val headers = List("lot", "wafer", "x", "y", "measurement", "value", "yield")
    val row = List("KT5379.10", "12", "-5", "-1", "testitem", "-7.18e-002", "No limit")
    val projectId = UUID.randomUUID()
    val expect = TestData(projectId, "KT5379.10", "12", -5, -1, "testitem", Some(-0.0718), Some("No limit"))
    val actual = LoadTestData.createTestData(headers, row, projectId)
    assert(expect == actual)
  }

}
