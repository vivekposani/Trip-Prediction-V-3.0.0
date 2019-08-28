package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql._

object VariableDynamic extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
  val Spark: SparkSession = getSparkSession()
  import Spark.implicits._

  val PlazaList = getInputPlazaList.collect().toList
  println("Input Plaza File Load Success")
  val InputData = getInputData.persist(StorageLevel.DISK_ONLY)
  println("Input Data File Load Success")

  PlazaList.map { x =>
    val PlazalistSplit = x.mkString.split(";")
    val inputPlaza = PlazalistSplit(0)
    //    "60001"
    val PerfomanceDate = PlazalistSplit(1)
    //    "2018-08-10"

    println("Started Running for Plaza:" + inputPlaza)

    val CustomInputData = InputData.filter($"PLAZACODE" === inputPlaza)
      .filter(x => x(2).toString().substring(0, 10) < PerfomanceDate).persist(StorageLevel.DISK_ONLY)
    println("CustomInputData Is Ready")

    val DistinctTag = CustomInputData.map(x => (x(0)).toString()).distinct().toDF("tag")

    val daysOfProportion = DaysOfProportion(CustomInputData, inputPlaza, PerfomanceDate)
    val pre1to7 = Pre1to7(CustomInputData, inputPlaza, PerfomanceDate)
    val samestate = SameState(CustomInputData, inputPlaza, PerfomanceDate)
    val discount = Discount(CustomInputData, inputPlaza, PerfomanceDate)
    val txnOnPerformanceDate = TxnOnPerformanceDate(InputData, inputPlaza, PerfomanceDate)
    val lastPlaza = LastPlaza(InputData, inputPlaza, PerfomanceDate)
    val distanceFromPreviousTxn = DistanceFromPreviousTxn(InputData, inputPlaza, PerfomanceDate)

    val variables = DistinctTag.join(txnOnPerformanceDate, Seq("tag"), "left_outer")
      .join(distanceFromPreviousTxn, Seq("tag"), "left_outer")
      .join(daysOfProportion, Seq("tag"), "left_outer")
      .join(pre1to7, Seq("tag"), "left_outer")
      .join(samestate, Seq("tag"), "left_outer")
      .join(discount, Seq("tag"), "left_outer")
      .join(lastPlaza, Seq("tag"), "left_outer")
      .na.fill(0)

    writeToCSV(variables, inputPlaza, PerfomanceDate)

  }

  def getSparkSession() = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.task.maxFailures", "6")
      //      .master("spark://192.168.70.32:7077")
      //      .config("spark.submit.deployMode", "cluster")
      //      .config("spark.executor.memory", "36g")
      //      .config("spark.driver.port", "8083")
      //      .config("spark.executor.port", "8084")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.32:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputPlazaList = {

    val spark = getSparkSession()
    spark.read.option("header", "true").csv("hdfs://192.168.70.32:9000/vivek/INSIGHT/CSV/Plaza.txt")

  }

  def getInputData = {

    val spark = getSparkSession()
    spark.read.option("header", "true").csv("hdfs://192.168.70.32:9000/vivek/INSIGHT/CSV/TagPlazaTimeStateDiscountClassTxn.csv")

  }

  def writeToCSV(df: DataFrame, plaza: String, date: String): Unit = {

    val folder = "hdfs://192.168.70.32:9000/vivek/Implementation/" + plaza + "/" + date + "/"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)

  }

  val t1 = System.currentTimeMillis()
  print((t1 - t0).toFloat / 1000)

}
    