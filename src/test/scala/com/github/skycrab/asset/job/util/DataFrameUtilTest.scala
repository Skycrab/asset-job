package com.github.skycrab.asset.job.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.{Before, Test}

/**
  * Created by yihaibo on 2019-11-07.
  */
class DataFrameUtilTest {
  lazy val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .getOrCreate()

  var df: DataFrame = _

  @Before
  def before(): Unit = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    df = Seq(User("Andy", 32, Array("lilei", "hanmeimei"))).toDF()
  }

  @Test
  def isStringTypeTest(): Unit = {
    assert(!DataFrameUtil.isStringType(df, "age"))
    assert(DataFrameUtil.isStringType(df, "name"))
    assert(!DataFrameUtil.isStringType(df, "love"))
  }


  @Test
  def isAtomicTypeTest(): Unit = {
    assert(DataFrameUtil.isAtomicType(df, "age"))
    assert(DataFrameUtil.isAtomicType(df, "name"))
    assert(!DataFrameUtil.isAtomicType(df, "love"))
  }

}

case class User(name: String, age: Long, love: Array[String])
