package com.github.billowen.wat.spark

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object LoadTestItems {
  def convert(headers: List[String], data: RDD[String], project_id: UUID, sc: SparkContext): RDD[TestItem] = {
    val itemRDD = data.map(_.split(","))
      .map(row => row.map(col => col.trim))
      .map(row => createATestItem(project_id, headers, row.toList, sc))
    itemRDD
  }

  def createATestItem(project_id: UUID, headers: List[String], cellData: List[String], sc: SparkContext): TestItem = {
    val testItem = TestItem(project_id)
    for (i <- cellData.indices) {
      headers(i) match {
        case "item" => testItem.name = cellData(i)
        case "unit" => testItem.unit = cellData(i)
        case "measurementType" => testItem.measure_type = cellData(i)
        case "formula" => testItem.formula = cellData(i)
        case "target" => testItem.target = cellData(i).toDouble
        case "specLow" => testItem.hard_low = cellData(i).toDouble
        case "specHigh" => testItem.hard_high = cellData(i).toDouble
        case "controlLow" => testItem.soft_low = cellData(i).toDouble
        case "controlHigh" => testItem.soft_high = cellData(i).toDouble
        case "structureName" => {
          val structRDD = sc.cassandraTable("syms_wat_database", "duts_by_name")
            .select("dut_id").where("name=?, project_id=?", cellData(i), project_id)
          if (structRDD.count() != 0) {
            testItem.dut_id = structRDD.first().getUUID("dut_id")
          }
        }
      }
    }
    testItem
  }
}
