package com.github.skycrab.asset.job

import java.io.File
import java.time.LocalDate

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.junit.Test

/**
 * Created by yihaibo on 2019-11-07.
 */
class SparkTest {
  @Test
  def test(): Unit = {
    val spark = SparkSession
            .builder
            .master("local")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .appName("test").getOrCreate()

    try {
      val logData = spark.read.textFile("/etc/hosts").cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      println("success")
    }catch{
      case e : Exception =>
        val msg = ExceptionUtils.getFullStackTrace(e);
        println("failed:" + msg)
        throw e
    }
  }

  @Test
  def testLocalLoad(): Unit = {
    val file = new File(getClass.getClassLoader.getResource("application.properties").getFile)
    val path = file.getParent

    println(path)
    val samplePath = path + "/sample/"
    val spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    val createSql = s"create table sample(uid long, age long, name string, last_login_time string, dt string) using csv options (path '$samplePath') partitioned by (dt)"
    println(createSql)
    spark.sql(createSql)
    spark.sql("ALTER TABLE sample RECOVER PARTITIONS")

    spark.sql("show create table sample").show()
    spark.sql("show partitions sample").show()
    spark.sql("select * from sample limit 10").show()
  }

  @Test
  def testLocal(): Unit = {
    val file = new File(getClass.getClassLoader.getResource("application.properties").getFile)
    val path = file.getParent

    val spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    val createSql = "create table sample(uid long, age long, dt string) using csv options (path '/tmp/sample') partitioned by (dt)"
    val insertSql = "insert OVERWRITE table sample partition(dt='20200714') select 1, 10 "

    println(createSql)
    spark.sql(createSql)
    spark.sql(insertSql)

    spark.sql("show create table sample").show()
    spark.sql("show partitions sample").show()
    spark.sql("select * from sample limit 10").show()
  }

  @Test
  def testLocal1(): Unit = {
    val sixthOfJanuary = LocalDate.of(2014, 1, 6)

    // add two months and five days to 2014-01-06, result is 2014-03-11
    val eleventhOfMarch = sixthOfJanuary.plusDays(150)
    print(eleventhOfMarch)
  }


}
