package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.{ LocalDate, Period }
import org.apache.spark.sql.types.IntegerType

object DistanceFromPreviousTxn {

  case class Record(tag: String, plaza: String, date: String, time: String)

  def apply(InputData: Dataset[Row], inputPlaza: String, PerfomanceDate: String) = {
    val Spark:SparkSession = getSparkSession()
    import Spark.implicits._

    val prevDay = LocalDate.parse(PerfomanceDate).minusDays(1).toString()

    val customizedInputData = InputData
      .map(x => (x(0).toString(), x(1).toString(), x(2).toString().substring(0, 10), x(2).toString().substring(10)))
      .filter(x => (x._3 == prevDay))
      .distinct
      .toDF("tag", "plaza", "date", "time")

    val tagWithLastPlaza = customizedInputData.as[Record]
      .groupByKey(x => (x.tag))
      .reduceGroups((x, y) => if (x.time > y.time) x else y)
      .map(x => (x._1, x._2.plaza)).toDF("tag", "plaza")

    val plazaDistance = getplazaDistance(Spark)
      .filter(x => x._1 == inputPlaza).map(x => (x._2, x._3))
      .toDF("plaza", "distance")

    val plazaWithDistance = tagWithLastPlaza.
      join(plazaDistance, Seq("plaza"))

    val plazaDistanceVariables = plazaWithDistance
      .select(
        $"tag",
        lit($"distance" < 500).cast(IntegerType) as "nearer",
        lit($"distance" > 1000).cast(IntegerType) as "distant")

    plazaDistanceVariables

  }

  def getplazaDistance(Spark: SparkSession) = {

    val distanceRDD = Spark.sparkContext.textFile("hdfs://192.168.70.32:9000/vivek/INSIGHT/CSV/PlazaCodeDistance.txt").map(_.split(",")).map(x => (x(0), x(1), x(2).toFloat))

    distanceRDD
  }

  def getSparkSession() = {
    SparkSession
      .builder
      .appName("SparkSQL")
      //      .master("local[*]")
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