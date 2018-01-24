package com.github.billowen.wat.spark

import java.io.FileNotFoundException
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.datastax.spark.connector._
import com.github.billowen.wat.spark.exception.{IllegalFileFormatException, ProjectNotFoundException}

import scala.util.Try

object LoadDuts {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LoadDuts <file> <project>")
      System.exit(1)
    }

    val fileName = args(0)
    val projectName = args(1)
    val conf = new SparkConf(true).setAppName("Load Duts")
    val sc = new SparkContext(conf)
    try {
      load(projectName, fileName, sc)
    } catch {
      case _ : FileNotFoundException => println(s"File $fileName not found")
      case ex : ProjectNotFoundException => println(ex.msg)
      case ex : IllegalFileFormatException => print(ex.msg)
    }
    sc.stop()
  }

  def load(projectName: String, file: String, sc: SparkContext): Unit = {
    val projectRDD = sc.cassandraTable("syms_wat_database", "projects_by_name")
      .select("project_id")
      .where("project_name=?", projectName)
    if (projectRDD.count() == 0)
      throw ProjectNotFoundException(s"The $projectName is not existed")
    else {
      val projectId = projectRDD.first().get[UUID]("project_id")
      val input = sc.textFile(file)
      val headerLine =  input.first() // extract header
      val headers = headerLine.split(",").map(s => s.trim)
      val data = input.filter(row => row != headerLine)
      convert(headers.toList, data, projectId)
        .saveToCassandra("syms_wat_database", "duts",
          SomeColumns("project_id", "dut_name", "module", "design_type", "attributes"))
    }
  }
  def convert(headers: List[String], data: RDD[String], projectId:UUID): RDD[Dut] = {
    val cellNameInd = headers.indexOf("cellName")
    if (cellNameInd == -1) throw IllegalFileFormatException("Load duts: can not find the cellName column.")
    data.map(_.split(","))
      .map(row => Try(createDut(headers, row.toList, projectId)))
      .filter(_.isSuccess)
      .map(_.get)
  }

  def createDut(headers: List[String], row: List[String], projectId: UUID) : Dut = {
    val cellNameInd = headers.indexOf("cellName")
    if (cellNameInd == -1) throw IllegalFileFormatException("Load duts: can not find the cellName column.")
    if (row.lengthCompare(cellNameInd) <= 0) throw IllegalFileFormatException("Load duts: can not find data of cellName")
    if (row(cellNameInd) == "") throw IllegalFileFormatException("Load duts: the cellName can not be empty.")
    val dut = Dut(projectId, row(cellNameInd))
    for (i <- headers.indices) {
      headers(i).trim match {
        case "module" => if (row.lengthCompare(i) > 0 &&  row(i).trim != "") dut.module = row(i).trim
        case "designType" => if (row.lengthCompare(i) > 0 && row(i).trim != "") dut.design_type = row(i).trim
        case "cellName" =>
        case _ => if (row.lengthCompare(i) > 0) dut.attributes += (headers(i).trim -> row(i).trim)
      }
    }
    dut
  }
}
