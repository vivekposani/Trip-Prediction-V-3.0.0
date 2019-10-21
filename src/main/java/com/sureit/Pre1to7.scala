package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import java.time.{ LocalDate, Period }
import scala.util.Try

object Pre1to7 {

  def apply(CustomInputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String) = {
    val Spark: SparkSession = getSparkSession()
    import Spark.implicits._

    val CustomizedInputData = CustomInputData
      .map(x => (x(0).toString(), x(2).toString().substring(0, 10)))
      .distinct()

    val customizedInputDataDF = CustomizedInputData.toDF("tag", "time")
      .withColumn("time", Try { to_date($"time") }.getOrElse(to_date(lit("2010-04-04"))))
      .withColumn("perf_date", to_date(lit(PerfomanceDate)))
      .filter($"time" >= date_add($"perf_date", -7))
      .distinct

    implicit def bool2int(b: Boolean) = if (b) 1 else 0
    val prev = customizedInputDataDF
      .select(
        $"tag",
        lit(datediff($"perf_date", $"time") === 1).cast(IntegerType) as "prev1",
        lit(datediff($"perf_date", $"time") === 2).cast(IntegerType) as "prev2",
        lit(datediff($"perf_date", $"time") === 3).cast(IntegerType) as "prev3",
        lit(datediff($"perf_date", $"time") === 4).cast(IntegerType) as "prev4",
        lit(datediff($"perf_date", $"time") === 5).cast(IntegerType) as "prev5",
        lit(datediff($"perf_date", $"time") === 6).cast(IntegerType) as "prev6",
        lit(datediff($"perf_date", $"time") === 7).cast(IntegerType) as "prev7")

    val prevGroup = prev.groupBy("tag").agg(sum($"prev1").alias("prev1"), sum($"prev2").alias("prev2"),
      sum($"prev3").alias("prev3"), sum($"prev4").alias("prev4"),
      sum($"prev5").alias("prev5"), sum($"prev6").alias("prev6"),
      sum($"prev7").alias("prev7"))

    prevGroup

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
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.24:9000/vivek/temp")
      .config("spark.local.dir", "vivek/temp")
      .getOrCreate()
  }

}