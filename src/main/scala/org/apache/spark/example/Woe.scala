package spark.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.WoeBinning
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by yihaibo on 18/10/29.
  */
object Woe {
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
      .load("src/main/resources/data.csv")
      .repartition(1)
    df.createOrReplaceTempView("woe")
    df.show(2,false)

    continuous(spark, df)
//    discrete(spark, df)
  }

  def continuous(spark: SparkSession, df: DataFrame): Unit = {
    val df2= spark.sql("select * from woe where service_score is null")
    df2.show(2,false)

    // 连续性变量
    val woeBinning = new WoeBinning()
      .setInputCol("issue_age")
      .setOutputCol("service_score_woe")
      .setLabelCol("is_bad")
      .setContinuous(true)

    val woeModel = woeBinning.fit(df)
    val result = woeModel.transform(df)
    println(woeModel.labelWoeIv.label.toList)
    println(woeModel.labelWoeIv.woeIvGroup)
    result.show()
  }

  def discrete(spark: SparkSession, df: DataFrame): Unit = {
    // 离散变量
    val woeBinning = new WoeBinning()
      .setInputCol("city_level")
      .setOutputCol("city_level_woe")
      .setLabelCol("is_bad")
      .setContinuous(false)

    val woeModel = woeBinning.fit(df)
    val result = woeModel.transform(df)
    println(woeModel.labelWoeIv.label.toList)
    println(woeModel.labelWoeIv.woeIvGroup)
    result.show()
  }
}