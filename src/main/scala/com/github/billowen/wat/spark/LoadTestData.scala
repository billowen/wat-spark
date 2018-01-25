package com.github.billowen.wat.spark

import java.util.UUID

import com.github.billowen.wat.spark.exception.{IllegalFileFormatException, ProjectNotFoundException}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import scala.util.Try

object LoadTestData {


  def load(projectName: String, file: String, sc: SparkContext) : Unit = {
    val projectRDD = sc.cassandraTable("syms_wat_database", "projects_by_name")
      .select("project_id")
      .where("project_name=?", projectName)
    if (projectRDD.count() == 0)
      throw ProjectNotFoundException(s"The $projectName is not existed")
    val projectId = projectRDD.first().get[UUID]("project_id")
    val input = sc.textFile(file)
    val headerLine =  input.first() // extract header
    val headers = headerLine.split(",").map(_.trim)
    val data = input.filter(row => row != headerLine).map(_.split(","))
    convert(headers.toList, data, projectId)
      .saveToCassandra("syms_wat_database", "test_data",
        SomeColumns("project_id", "lot", "wafer", "x", "y", "measurement", "value", "yield_type"))

    // Update the statistic information of wafer and lot
    val lotInd = headers.indexOf("lot")
    val waferInd = headers.indexOf("wafer")
    data.map(row => Try((row(lotInd).trim, row(waferInd).trim)))
      .filter(_.isSuccess).map(_.get).distinct()
      .map(item => waferSummary(projectId, item._1, item._2, sc))
      .saveToCassandra("syms_wat_database", "wafers", SomeColumns(
        "project_id", "lot", "wafer", "die_cnt", "test_cnt", "yield_rate"
      ))
    data.map(row => Try(row(lotInd).trim))
      .filter(_.isSuccess)
      .map(_.get)
      .map(lotSummary(projectId, _, sc))
      .saveToCassandra("syms_wat_database", "lots", SomeColumns(
        "project_id", "lot", "wafer_cnt", "yield_rate"
      ))
  }

  def convert(headers: List[String], data: RDD[Array[String]], projectId: UUID) : RDD[TestData] = {
    if (headers.indexOf("lot") == -1)
      throw IllegalFileFormatException("Load test data: missing column of lot")
    if (headers.indexOf("wafer") == -1)
      throw IllegalFileFormatException("Load test data: missing column of wafer")
    if (headers.indexOf("x") == -1)
      throw IllegalFileFormatException("Load test data: missing column of x")
    if (headers.indexOf("y") == -1)
      throw IllegalFileFormatException("Load test data: missing column of y")
    if (headers.indexOf("measurement") == -1)
      throw IllegalFileFormatException("Load test data: missing column of measurement")
    data.map(row => Try(createTestData(headers, row.toList, projectId)))
      .filter(_.isSuccess)
      .map(_.get)
  }

  def createTestData(headers: List[String], row: List[String], projectId: UUID) : TestData = {
    val lot =
      if (!validateCell("lot", headers, row))
        throw IllegalFileFormatException("Load test data: can not find lot data")
      else
        row(headers.indexOf("lot")).trim
    val wafer =
      if (!validateCell("wafer", headers, row))
        throw IllegalFileFormatException("Load test data: can not find wafer data")
      else
        row(headers.indexOf("wafer")).trim
    val x =
      if (!validateCell("x", headers, row))
        throw IllegalFileFormatException("Load test data: can not find die x data")
      else
        row(headers.indexOf("x")).trim.toInt
    val y =
      if (!validateCell("y", headers, row))
        throw IllegalFileFormatException("Load test data: can not find die y data")
      else
        row(headers.indexOf("y")).trim.toInt
    val measurement =
      if (!validateCell("measurement", headers, row))
        throw IllegalFileFormatException("Load test data: can not find measurement data")
      else
        row(headers.indexOf("measurement")).trim

    val test = TestData(projectId, lot, wafer, x, y, measurement)

    if (validateCell("value", headers, row)) {
      try {
        test.value = Some(row(headers.indexOf("value")).trim.toDouble)
      } catch {
        case _ : Throwable =>
      }
    }
    if (validateCell("yield", headers, row)) {
      test.yield_type = Some(row(headers.indexOf("yield")).trim)
    }

    test
  }

  def updateStatistics(headers: List[String], data: RDD[Array[String]], projectId: UUID): Unit = {
    val lotInd = headers.indexOf("lot")
    val waferInd = headers.indexOf("wafer")
    val waferRdd = data.map(row => Try({
      (row(lotInd).trim, row(waferInd).trim)
    })).filter(_.isSuccess).map(_.get).distinct().persist()
  }

  def waferSummary(projectId: UUID, lotName: String, waferName: String, sc: SparkContext): Wafer = {
    val waferDataRdd = sc.cassandraTable("syms_wat_database", "test_data").select("x", "y")
      .where("project_id=?", projectId)
      .where("lot=?", lotName)
      .where("wafer=?", waferName).persist()

    val itemAcc = sc.longAccumulator(s"Test item count: $lotName-$waferName")
    waferDataRdd.foreach(_ => itemAcc.add(1))
    val itemCnt = itemAcc.value
    val dieAcc = sc.longAccumulator(s"Test die count: $lotName-$waferName")
    waferDataRdd.distinct().foreach(_ => dieAcc.add(1))
    val dieCnt = dieAcc.value

    Wafer(projectId, lotName, waferName, dieCnt, itemCnt)
  }

  def lotSummary(projectId: UUID, lotName: String, sc: SparkContext) : Lot = {
    var waferSummaryRdd = sc.cassandraTable("syms_wat_database", "wafers").select("wafer")
      .where("project_id=?", projectId)
      .where("lot=?", lotName)
    val waferAcc = sc.longAccumulator(s"Wafer count: $lotName")
    waferSummaryRdd.foreach(_ => waferAcc.add(1))
    Lot(projectId, lotName, waferAcc.value)
  }

  def validateCell(colName: String, headers: List[String], row: List[String]) : Boolean = {
    if (headers.indexOf(colName) == -1
      || row.lengthCompare(headers.indexOf(colName)) <= 0
      || row(headers.indexOf(colName)) == "")
      false
    else
      true
  }


}
