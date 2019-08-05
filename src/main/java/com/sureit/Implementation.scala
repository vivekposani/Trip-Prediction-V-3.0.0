
package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.ml.classification.LogisticRegression

object Implementation extends App {
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
    //    "2018-10-11"
    val Beta = PlazalistSplit(2).split(",")
    val Cutoff = PlazalistSplit(3)

    //    println(Beta)

    println("Started Running for Plaza:" + inputPlaza)

    val CustomInputData = InputData.filter($"PLAZACODE" === inputPlaza)
      .filter(InputData.col("EXITTXNDATE").substr(1, 10) < PerfomanceDate).persist(StorageLevel.DISK_ONLY)
    println("CustomInputData Is Ready")
    //  CustomInputData.show(10)
    //  CustomInputData.printSchema()
    //  InputData.printSchema()

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

    //  println("*************************************************************************")
    //  variables.show(10)

    //  val mlr = new LogisticRegression()
    //    .setMaxIter(10)
    //    .setRegParam(0.3)
    //    .setElasticNetParam(0.8)
    //    .setFamily("multinomial")
    //
    //  val mlrModel = mlr.fit(variables)
    //
    //  println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    //  println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    val implementationOut = Probability(variables, Beta, Cutoff)

    writeToCSV(implementationOut, inputPlaza, PerfomanceDate)

    println("Plaza " + inputPlaza + " Done")

  }

  //  val Performance = PerformanceMatric
  //  val format = new SimpleDateFormat("yyyy-MM-dd")
  //  val Date = format.format(Calendar.getInstance().getTime())
  //
  //  write(Performance, Date)

  def getSparkSession() = {
    SparkSession
      .builder
      .appName("SparkSQL")
      //      .master("local[*]")
      .master("spark://192.168.70.21:7077")
      .config("spark.submit.deployMode", "client")
      .config("spark.task.maxFailures", "6")
      .config("spark.executor.memory", "36g")
      .config("spark.driver.port", "8083")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.21:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputPlazaList = {
    val spark = getSparkSession()
    spark.read.option("header", "true").textFile("hdfs://192.168.70.21:9000/vivek/INSIGHT/CSV/Plaza.csv")
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