package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Discount {

  case class Record(tag: String, time: String, discount: String)

  def apply(CustomInputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String, Spark: SparkSession) = {
    import Spark.implicits._

    val CustomizedInputData = CustomInputData.filter($"Discountcode" === "0")
      .map(x => (x(0), x(2), x(4)))
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
}