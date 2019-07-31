package com.sureit

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.{ LocalDate, Period }

object DaysOfProportion {

  def apply(CustomInputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String, Spark: SparkSession) = {
    import Spark.implicits._

    val MaxDate = LocalDate.parse(PerfomanceDate).minusMonths(3).toString()
    val CustomizedInputData = CustomInputData
      .map(x => (x(0).toString(), x(2).toString().substring(0, 10)))
      .distinct()
      .toDF("tag", "time")
      .withColumn("EXITTXNDATE", to_date($"time"))

    val tagCountMinDF = CustomizedInputData.groupBy($"tag").agg(count($"tag").alias("count"), min($"time").alias("start_date"))
    val tagCountDiff = tagCountMinDF.withColumn("diff", datediff(to_date(lit(PerfomanceDate)), $"start_date"))
    val daysProportionDF = tagCountDiff.select($"tag", bround(($"count" / $"diff"), 7) as "dp").withColumn("dplog", bround(log(10, "dp"), 7))

    daysProportionDF
  }

}
