package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TxnOnPerformanceDate {

  def apply(InputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String) = {
    val Spark: SparkSession = getSparkSession()
    import Spark.implicits._

    val tagWithTxnOnPerformanceDateDF = InputData.filter($"PLAZACODE" === inputPlaza)
      .map(x => (x(0).toString(), x(2).toString().substring(0, 10)))
      .filter(x => x._2 == PerfomanceDate)
      .distinct()
      .map(x => (x._1, 1))
      .toDF("tag", "txn_on_input_date")

    tagWithTxnOnPerformanceDateDF

  }

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

}