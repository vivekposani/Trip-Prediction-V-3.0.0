package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TxnOnPerformanceDate {

  def apply(InputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String, Spark: SparkSession) = {
    import Spark.implicits._

    val tagWithTxnOnPerformanceDateDF = InputData.filter($"PLAZACODE" === inputPlaza)
      .map(x => (x(0).toString(), x(2).toString().substring(0, 10)))
      .distinct()
      .filter($"EXITTXNDATE" === PerfomanceDate)
      .map(x => ($"TAGID", 1))
      .toDF("tag", "txn_on_input_date")

    tagWithTxnOnPerformanceDateDF

  }
}