package com.github.skycrab.asset.job

import org.apache.log4j.{Level, Logger}
import com.github.skycrab.asset.job.handler.impl.CommonIvMetricHandler
import org.apache.spark.sql.SparkSession

/**
  * Created by yihaibo on 18/10/29.
  */
object IvTest {
  def main(args: Array[String]) : Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local")
      .appName("woe").getOrCreate()

    val df = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("src/test/resources/titanic_data.csv")
      .na.fill(-999.0)

    df.show(2, false)

    val h = new CommonIvMetricHandler()

    h.handle(df, "Pclass", "Survived")
    h.handle(df, "Age", "Survived")
    h.handle(df, "Fare", "Survived")

  }
}