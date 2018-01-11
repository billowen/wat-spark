package com.github.billowen.wat.spark

import java.io.FileNotFoundException
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession

object LoadDuts {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LoadDuts <file> <project>")
      System.exit(1)
    }

    val fileName = args(0)
    val projectName = args(1)
    println(fileName)
    val conf = new SparkConf(true).setAppName("Load Duts")
    val sc = new SparkContext(conf)
    try {
      val error = load(projectName, fileName, sc)
    } catch {
      case ex : FileNotFoundException => println(s"File $fileName not found")
    }
    sc.stop()
  }

  def load(projectName: String, file: String, sc: SparkContext): String = {
    val projectRDD = sc.cassandraTable("syms_wat_database", "projects_by_name")
      .select("project_id")
      .where("name=?", projectName)
    var error = ""
    if (projectRDD.count() == 0)
      error = "The project does not exist."
    else {
      val projectId = projectRDD.first().get[UUID]("project_id")
      val input = sc.textFile(file)
      val header =  input.first() // extract header
      val cols = header.split(",")
      if (!cols.contains("cellName"))
        error = "Missing column: cellName"
      else if (!cols.contains("module"))
        error = "Missing column： module"
      else if (!cols.contains("designType"))
        error = "Missing column: designType"
      else {
        val data = input.filter(row => row != header)
        val cols = header.split(",").map(col => col.trim).toList
        val dutRdd = convert(cols, data, projectId)
        dutRdd.saveToCassandra("syms_wat_database", "duts",
          SomeColumns("project_id", "dut_id", "name", "module", "design_type", "attributes"))
      }
    }
    error
  }
  def convert(headers: List[String], data: RDD[String], project_id:UUID): RDD[Dut] = {
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
        Dut(project_id, cellName, module, designType, attributes)
      })
    dutRDD
  }
}
