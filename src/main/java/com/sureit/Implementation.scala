package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.util.Calendar

object Implementation extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val t0 = System.currentTimeMillis()
  val Spark: SparkSession = getSparkSession()
  import Spark.implicits._

  val PlazaList = getInputPlazaList.persist(StorageLevel.DISK_ONLY)
  println("Input Plaza File Load Success")
  val InputData = getInputData.persist(StorageLevel.DISK_ONLY)
  println("Input Data File Load Success")

  PlazaList.foreach { x =>
    val PlazalistSplit = x.mkString.split(";")
    val inputPlaza = PlazalistSplit(0)
    //    "60001"
    val PerfomanceDate = PlazalistSplit(1)
    //    "2018-08-10"
    val Beta = PlazalistSplit(2).split(",")
    val Cutoff = PlazalistSplit(3)

    println("Started Running for Plaza:" + inputPlaza)

    val CustomInputData = InputData.filter($"PLAZACODE" === inputPlaza && $"EXITTXNDATE".toString().substring(0, 10) < PerfomanceDate).persist(StorageLevel.DISK_ONLY)
    println("CustomInputData Is Ready")

    val DistinctTag = CustomInputData.map(x => (x(0)).toString()).distinct().toDF("tag")

    val daysOfProportion = DaysOfProportion(CustomInputData, inputPlaza, PerfomanceDate, Spark)
    val pre1to7 = Pre1to7(CustomInputData, inputPlaza, PerfomanceDate, Spark)
    val samestate = SameState(CustomInputData, inputPlaza, PerfomanceDate, Spark)
    val discount = Discount(CustomInputData, inputPlaza, PerfomanceDate, Spark)
    val txnOnPerformanceDate = TxnOnPerformanceDate(InputData, inputPlaza, PerfomanceDate, Spark)
    val lastPlaza = LastPlaza(InputData, inputPlaza, PerfomanceDate, Spark)
    val distanceFromPreviousTxn = DistanceFromPreviousTxn(InputData, inputPlaza, PerfomanceDate, Spark)

    val variables = DistinctTag.join(txnOnPerformanceDate, Seq("tag"), "left outer")
      .join(distanceFromPreviousTxn, Seq("tag"), "left outer")
      .join(daysOfProportion, Seq("tag"), "left outer")
      .join(pre1to7, Seq("tag"), "left outer")
      .join(samestate, Seq("tag"), "left outer")
      .join(discount, Seq("tag"), "left outer")
      .join(lastPlaza, Seq("tag"), "left outer")
      .na.fill(0)

    val implementationOut = Probability(variables, Beta, Cutoff, Spark)

    writeToCSV(implementationOut, inputPlaza, PerfomanceDate)

    println("Plaza " + inputPlaza + " Done")

  }

  val Performance = PerformanceMatric(Spark)
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val Date = format.format(Calendar.getInstance().getTime())

  write(Performance, Date)

  def getSparkSession() = {
    SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      //      .config("spark.submit.deployMode", "cluster")
      //      .config("spark.executor.memory", "36g")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.21:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputPlazaList = {
    val spark = getSparkSession()
    spark.read.option("header", "true").csv("hdfs://192.168.70.21:9000/vivek/INSIGHT/CSV/Plaza.txt")
  }

  def getInputData = {
    val spark = getSparkSession()
    spark.read.option("header", "true").csv("hdfs://192.168.70.21:9000/vivek/INSIGHT/CSV/TagPlazaTimeStateDiscountClassTxn.csv")
  }

  def writeToCSV(df: DataFrame, plaza: String, date: String): Unit = {

    val folder = "hdfs://192.168.70.21:9000/vivek/Implementation/" + plaza + "/" + date + "/"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)

  }

  def write(df: DataFrame, date: String) = {
    val folder = "hdfs://192.168.70.21:9000/vivek/PerformanceMatrix/" + date + "/"
    df.repartition(1).write.format("csv").mode("overwrite").option("header", "true").save(folder)

  }

  val t1 = System.currentTimeMillis()
  print((t1 - t0).toFloat / 1000)

}