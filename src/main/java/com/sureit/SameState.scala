package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SameState {

  def apply(CustomInputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String, Spark: SparkSession) = {
    import Spark.implicits._

    val CustomizedInputData = CustomInputData
      .map(x => (x(0).toString(), x(3).toString()))
      .toDF("tag", "same_state")

    val trips = CustomizedInputData.groupBy("tag").agg(sum(col("tag"))).withColumnRenamed("sum(tag)", "tag_COUNT")
    val trips_count1 = trips.select($"tag", $"tag_COUNT").withColumn("TRIPS>1", when(col("tag_COUNT") > 1, lit(1)).otherwise(lit(0)))
    val TRIPS_COUNT = trips_count1.select("tag", "TRIPS>1")

    val distinctTag = CustomizedInputData.distinct
    val same_state = distinctTag.join(TRIPS_COUNT, Seq("tag"), "left outer")

    same_state

  }
}