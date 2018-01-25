package com.github.billowen.wat.spark

import java.io.FileNotFoundException
import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.github.billowen.wat.spark.exception.{IllegalFileFormatException, ProjectNotFoundException}

import scala.util.Try

object LoadTestItems {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LoadTestItems <file> <project>")
      System.exit(1)
    }

    val fileName = args(0)
    val projectName = args(1)
    val conf = new SparkConf(true)
      .setAppName("Load TestItems")
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
        .saveToCassandra("syms_wat_database", "test_items", SomeColumns(
          "project_id", "item", "measurement", "dut_name", "formula", "unit",
          "target", "soft_low", "soft_high", "hard_low", "hard_high"
        ))
    }
  }

  def convert(headers : List[String], data: RDD[String], projectId: UUID) : RDD[TestItem] = {
    val cellNameInd = headers.indexOf("item")
    if (cellNameInd == -1) throw IllegalFileFormatException("Load test items: can not find the item column.")
    data.map(_.split(","))
      .map(row => Try(createTestItem(headers, row.toList, projectId)))
      .filter(_.isSuccess)
      .map(_.get)
  }

  def createTestItem(headers: List[String], cellData: List[String], projectId: UUID): TestItem = {
    val itemInd = headers.indexOf("item")
    if (itemInd == -1) throw IllegalFileFormatException("Load test items: can not find item column")
    if (cellData.lengthCompare(itemInd) <= 0) throw IllegalFileFormatException("Load test items: can not find data of item")
    if (cellData(itemInd) == "") throw IllegalFileFormatException("Load test items: the item can not be empty")

    val testItem = TestItem(projectId, cellData(itemInd).trim)
    for (i <- cellData.indices) {
      if (cellData.lengthCompare(i) > 0) {
        headers(i).trim match {
          case "unit" => testItem.unit = cellData(i).trim
          case "measurementType" => testItem.measurement = cellData(i).trim
          case "formula" => testItem.formula = cellData(i).trim
          case "target" => testItem.target = try { Some(cellData(i).trim.toDouble) } catch { case _ : Throwable => None}
          case "specLow" => testItem.hard_low = try { Some(cellData(i).trim.toDouble) } catch { case _ : Throwable => None}
          case "specHigh" => testItem.hard_high = try { Some(cellData(i).trim.toDouble) } catch { case _ : Throwable => None}
          case "controlLow" => testItem.soft_low = try { Some(cellData(i).trim.toDouble) } catch { case _ : Throwable => None}
          case "controlHigh" => testItem.soft_high = try { Some(cellData(i).trim.toDouble) } catch { case _ : Throwable => None}
          case "structureName" => testItem.dut_name = cellData(i).trim
          case _ => {}
        }
      }
    }
    testItem
  }
}
