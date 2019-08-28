package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LastPlaza {

  def apply(InputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String) = {
    val Spark: SparkSession = getSparkSession()
    import Spark.implicits._

    val inputDataFiltered = InputData
      .filter(x => (x(2).toString().substring(0, 10) < PerfomanceDate))
      .map(x => (x(0).toString(), x(1).toString(), x(2).toString()))

    val lastPlazaTime = inputDataFiltered.groupByKey(x => x._1)
      .reduceGroups((x, y) => if (x._3 > y._3) { x } else { y })
      .map(x => x._2)
      .toDF("tag", "LastPlaza", "LastTime")

    lastPlazaTime

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

}