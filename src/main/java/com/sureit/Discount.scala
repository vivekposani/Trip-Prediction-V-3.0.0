package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Discount {

  case class Record(tag: String, time: String, discount: String)

  def apply(CustomInputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String) = {
    val Spark: SparkSession = getSparkSession()
    import Spark.implicits._

    val CustomizedInputData = CustomInputData.filter($"DISCOUNTCODE" > "0")
      .map(x => (x(0).toString, x(2).toString, x(4).toString))
      .toDF("tag", "time", "discount")

    val tagWithDiscount = CustomizedInputData.as[Record]
      .groupByKey(x => (x.tag))
      .reduceGroups((x, y) => if (x.time > y.time) { x } else { y })
      .map(x => (x._2.tag, x._2.discount, x._2.time))

    val tagWithDailyAndMonthly = tagWithDiscount.filter(x => x._2 != 3)
      .map(x => (x,
        (if (x._2 == "1") (1, 0) else if (x._2 == "2") (0, 1) else (0, 0))))
      .map(x => (x._1._1, x._2._1, x._2._2))
      .toDF("tag", "daily_pass", "monthly_pass")

    val local = tagWithDiscount.filter(x => x._2 == "3").map(x => (x._1, 1)).distinct.toDF("tag", "local")
    val discountVariables = tagWithDailyAndMonthly.join(local, Seq("tag"), "outer")

    discountVariables

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
