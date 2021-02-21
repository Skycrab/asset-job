package com.github.skycrab.asset.job.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Before
import org.springframework.beans.factory.annotation.Autowired

/**
  * Created by yihaibo on 2019-11-11.
  */
class SparkServiceTestBase {

  @Autowired
  var spark: SparkSession = _

  var df: DataFrame = _

  @Before
  def before(): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    val _spark = spark
    import _spark.implicits._

    df = Seq(
      Person("Andy", 10L),
      Person("Andy", 15L),
      Person("Andy", 30L),
      Person("Andy", 40L),
      Person("Andy", 42L),
      Person("Andy", 50L),
      Person("Andy", 60L),
      Person("Andy", 70L),
      Person("Andy", 30L),
      Person("Andy", 43L),
      Person("Andy", 0L),
      Person("Andy", 0L),
      Person("Andy", 0L),
      Person("Bob", 0L)
    ).toDF()

    df.createOrReplaceTempView("person")

  }

}

case class Person(name: String, age: java.lang.Long)
