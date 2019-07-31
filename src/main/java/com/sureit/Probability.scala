package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Probability {

  def apply(variable: DataFrame, beta: Array[String], cutoff: String, Spark: SparkSession) = {
    import Spark.implicits._

    val variableWithZ = variable
      .withColumn(
        "z",
        (lit(beta(0)) +
          lit(beta(1)) * $"nearer" +
          lit(beta(2)) * $"distant" +
          lit(beta(3)) * $"dp" +
          lit(beta(4)) * $"dplog" +
          lit(beta(5)) * $"prev1" +
          lit(beta(6)) * $"prev2" +
          lit(beta(7)) * $"prev3" +
          lit(beta(8)) * $"prev4" +
          lit(beta(9)) * $"prev5" +
          lit(beta(10)) * $"prev6" +
          lit(beta(11)) * $"prev7" +
          lit(beta(12)) * $"same_state" +
          lit(beta(13)) * $"daily_pass" +
          lit(beta(14)) * $"monthly_pass" +
          lit(beta(15)) * $"local"))

    val variableWithProb = variableWithZ
      .withColumn(
        "prob", bround((lit(1) / (lit(1) + (exp(lit(-1) * $"z")))), 4))

    val variableWithOutcome = variableWithProb.withColumn("event", (when($"prob" > cutoff, 1).otherwise(0)))
    variableWithOutcome

  }
}