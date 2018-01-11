package com.github.billowen.wat.spark

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
//import com.datastax.spark.connector._

object LoadDuts {
//  def load(projectName: String, file: String, sc: SparkContext): (Boolean, String) = {
//    val projectRDD = sc.cassandraTable("syms_wat_database", "projects_by_name")
//      .select("project_id")
//      .where("name=?", projectName)
//    var ret = (true, "")
//    if (projectRDD.count() == 0)
//      ret = (false, "The project does not exist.")
//    else {
//      val projectId = projectRDD.first().get[UUID]("project_id")
//      val input = sc.textFile(file)
//      val header =  input.first() // extract header
//      val cols = header.split(",")
//      if (!cols.contains("cellName"))
//        ret = (false, "Missing column: cellName")
//      else if (!cols.contains("module"))
//        ret = (false, "Missing column: module")
//      else if (!cols.contains("designType"))
//        ret = (false, "Missing column: module")
//    }
//    ret
//  }
  def load(headers: List[String], data: RDD[String]): RDD[Dut] = {
    val dutRDD = data.map(_.split(","))
      .map(row => {
        var cellName = ""
        var module =""
        var designType = ""
        var attributes : Map[String, String] = Map()
        for (i <- row.indices) {
          if (headers(i) == "cellName")
            cellName = row(i).trim
          else if (headers(i) == "module")
            module = row(i).trim
          else if (headers(i) == "designType")
            designType = row(i).trim
          else {
            attributes += (headers(i) -> row(i).trim)
          }
        }
        Dut(cellName, module, designType, attributes)
      })
    dutRDD
  }
}
