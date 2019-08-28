package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.{ LocalDate, Period }

object DaysOfProportion {

  def apply(CustomInputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String) = {
    val Spark: SparkSession = getSparkSession()
    import Spark.implicits._

    val MaxDate = LocalDate.parse(PerfomanceDate).minusMonths(3).toString()
    val CustomizedInputData = CustomInputData.filter(CustomInputData.col("EXITTXNDATE").substr(1, 10) < MaxDate)
      .map(x => (x(0).toString(), x(2).toString().substring(0, 10)))
      .distinct()
      .toDF("tag", "time")
      .withColumn("EXITTXNDATE", to_date($"time"))

    val tagCountMinDF = CustomizedInputData.groupBy($"tag").agg(count($"tag").alias("count"), min($"time").alias("start_date"))
    val tagCountDiff = tagCountMinDF.withColumn("diff", datediff(to_date(lit(PerfomanceDate)), $"start_date"))
    val daysProportionDF = tagCountDiff.select($"tag", bround(($"count" / $"diff"), 7) as "dp").withColumn("dplog", bround(log(10, "dp"), 7))

    daysProportionDF
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
