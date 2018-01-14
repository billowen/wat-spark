package com.github.billowen.wat.spark

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.github.billowen.wat.spark.exception.{IllegalFileFormatException, ProjectNotFoundException}
import org.apache.spark.storage.StorageLevel

object LoadTestItems {
//  def convert(headers: Array[String], data: RDD[String], project_id: UUID, sc: SparkContext): RDD[TestItem] = {
//    val itemRDD = data.map(_.split(","))
//      .map(row => row.map(col => col.trim))
//    itemRDD
//  }

  def load(projectName: String, file: String, sc: SparkContext): Unit = {
    val projectRDD = sc.cassandraTable("syms_wat_database", "projects_by_name")
      .select("project_id")
      .where("name=?", projectName).cache()
    var error = ""
    if (projectRDD.count() == 0)
      throw ProjectNotFoundException("The project is not existed: " + projectName)
    else {
      val projectId = projectRDD.first().get[UUID]("project_id")
      val input = sc.textFile(file)
      val header = input.first()
      val cols = header.split(",")
      if (!cols.contains("item"))
        throw IllegalFileFormatException("The test item definition files must contain column named item")
      else if (!cols.contains("structureName"))
        throw IllegalFileFormatException("The test item definition files must contain column named structureName")
      else {
        val data = input.filter(row => row != header).map(row => row.split(",")).persist(StorageLevel.MEMORY_AND_DISK)
        val structures = getAllStructure(cols, data).collect()
        var dutIds : Map[String, UUID] = Map()
        for (structure <- structures) {
          val structureRdd = sc.cassandraTable("syms_wat_database", "duts_by_name").select("dut_id")
            .where("name=?, project_id=?", structure, projectId)
          if (structureRdd.count() != 0)
            dutIds += (structure -> structureRdd.first().get[UUID]("dut_id"))
        }
        val testItemRdd = data.map(line => line.map(cell => cell.trim))
          .map(line => createTestItem(cols, line, projectId, dutIds))
          .saveToCassandra("syms_wat_database", "test_items", SomeColumns(
            "project_id", "test_id", "name", "measure_type", "dut_id", "formula", "unit",
            "target", "soft_low", "soft_high", "hard_low", "hard_high"
          ))
      }
    }
  }

  def getAllStructure(headers: Array[String], dataRdd: RDD[Array[String]]): RDD[String] = {
    dataRdd.map(lineData => {
        val colNum = headers.indexOf("structureName")
        lineData(colNum).trim
      })
      .distinct()
  }

  def createTestItem(headers: Array[String], cellData: Array[String], projectId: UUID, dutIds: Map[String, UUID]): TestItem = {
    val testItem = TestItem(project_id = projectId)
    for (i <- cellData.indices) {
      headers(i) match {
        case "item" => testItem.name = cellData(i)
        case "unit" => testItem.unit = cellData(i)
        case "measurementType" => testItem.measure_type = cellData(i)
        case "formula" => testItem.formula = cellData(i)
        case "target" => testItem.target = try {Some(cellData(i).toDouble)} catch { case _ : Throwable => None}
        case "specLow" => testItem.hard_low = try {Some(cellData(i).toDouble)} catch { case _ : Throwable => None}
        case "specHigh" => testItem.hard_high = try {Some(cellData(i).toDouble)} catch { case _ : Throwable => None}
        case "controlLow" => testItem.soft_low = try {Some(cellData(i).toDouble)} catch { case _ : Throwable => None}
        case "controlHigh" => testItem.soft_high = try {Some(cellData(i).toDouble)} catch { case _ : Throwable => None}
        case "structureName" => {
          if (dutIds.contains(cellData(i)))
            testItem.dut_id = Some(dutIds(cellData(i)))
        }
        case _ => {}
      }
    }
    testItem
  }
}
