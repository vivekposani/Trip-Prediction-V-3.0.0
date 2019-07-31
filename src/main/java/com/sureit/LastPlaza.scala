package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LastPlaza {

  def apply(InputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String, Spark: SparkSession) = {
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
}