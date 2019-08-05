package com.sureit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.SQLContext

object PerformanceMatric {

  def apply = {
    val Spark: SparkSession = getSparkSession()

    val plazalist = getInputPlaza.collect().toList

    val url = "jdbc:sqlserver://192.168.70.15:1433; database=SUREIT"
    val properties = new Properties()
    properties.put("user", "vivek")
    properties.put("password", "welcome123")
    properties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    //  val format = new SimpleDateFormat("yyyy-MM-dd")
    //  val performanceDate = format.format(Calendar.getInstance().getTime())
    //  val TestDate = LocalDate.parse(performanceDate).minusDays(1).toString()

    val x = plazalist.map { x =>

      val plazaWithBetaArray = x.split(";")
      val plaza = plazaWithBetaArray(0)
      val date = plazaWithBetaArray(1)
      //    println(date)

      val Input = getInputData(plaza, date)
        .mapPartitionsWithIndex {
          (idx, iter) => if (idx == 0) iter.drop(1) else iter
        }
      //    InputFiltered.take(5).foreach(println)
      val query = "(select distinct TAGID from SUREIT.CTP.INSIGHT where PLAZACODE = " + plaza + " and cast(EXITTXNDATE as date) = '" + date + "') Event"

      val Event = Spark.read.jdbc(url = url, table = query, properties).count()
      val Predict = Input.filter(x => x._3 == "1").count()

      val TrueNegitive = Input.filter(x => x._2 == "0").filter(x => x._3 == "0").count().toDouble
      val TruePositive = Input.filter(x => x._2 == "1").filter(x => x._3 == "1").count().toDouble
      val Type1 = Input.filter(x => x._2 == "0").filter(x => x._3 == "1").count().toDouble //FalsePositive
      val Type2 = Input.filter(x => x._2 == "1").filter(x => x._3 == "0").count().toDouble //FalseNegitive

      val Accuracy = (TrueNegitive + TruePositive) / (TruePositive + TrueNegitive + Type1 + Type2).toDouble
      val Sensitivity = TruePositive / (TruePositive + Type2).toDouble
      val FPR = Type1 / (Type1 + TrueNegitive).toDouble
      val OPR = TruePositive / (Type1 + TruePositive).toDouble
      val F1 = (2 * TruePositive) / ((2 * TruePositive) + Type1 + Type2).toDouble

      val Final = (plaza, date, Event, Predict, TrueNegitive, TruePositive, Type1, Type2, Accuracy, Sensitivity, FPR, OPR, F1)

      Final
      //    println(Final)

      //    val In1 = InputFiltered.toDF("Tag")
      //    InputFiltered.toDF("Tag", "Actual", "Predict").createOrReplaceTempView("temp")

      //    In1.show(10)

      //    val FalseNegitive = spark.sql("select count(*) as FalseNegitive from temp where Actual = 0 and Predict = 0")
      //    val TruePositive = spark.sql("select count(*) as TruePositive from temp where Actual = 1 and Predict = 1")
      //    val Type1 = spark.sql("select count(*) as Type1 from temp where Actual = 0 and Predict = 1")
      //    val Type2 = spark.sql("select count(*) as Type2 from temp where Actual = 1 and Predict = 0")

      //    val query = "(select TAGID, case when PLAZACODE = " + plaza + " and cast(EXITTXNDATE as date) = " + date + " then 1 else 0 end as event from SUREIT.CTP.INSIGHT) Event"
      //    val event = spark.read.jdbc(url=url, table=query, properties)
      //    event.show(10)

      //actual input
      //v1,v2,v3,v4
      //total

    }
    import Spark.sqlContext.implicits._

    val Output = Spark.sparkContext.parallelize(x).toDF("Plaza", "Date", "Event", "Predict", "TrueNegitive", "TruePositive", "Type1", "Type2", "Accuracy", "Sensitivity", "FPR", "OPR", "F1")

    Output

    //    val format = new SimpleDateFormat("yyyy-MM-dd")
    //    val Date = format.format(Calendar.getInstance().getTime())
    //    write(Output, Date)

  }

  def getSparkSession() = {
    SparkSession
      .builder
      .appName("SparkSQL")
      //      .master("local[*]")
      .master("spark://192.168.70.21:7077")
      .config("spark.submit.deployMode", "client")
      .config("spark.task.maxFailures", "6")
      .config("spark.executor.memory", "36g")
      .config("spark.driver.port", "8083")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.70.21:9000/vivek/temp")
      .getOrCreate()
  }

  def getInputData(plaza: String, date: String) = {
    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/Implementation/" + plaza + "/" + date + "/")
      .map(_.split(","))
      .map(x => (x(0), x(1), x(20)))
  }

  def getInputPlaza = {

    val spark = getSparkSession()
    spark.sparkContext.textFile("hdfs://192.168.70.7:9000/vivek/INSIGHT/CSV/Plaza.csv")

  }

}