package com.github.skycrab.asset.job.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * Created by yihaibo on 2019-11-07.
  */
object DataFrameUtil {

  /**
    * 判断是否是字符串时间类型
    * @param df
    * @param column
    * @return
    */
  def isDateOrDateTimeStringType(df: DataFrame, column: String): Boolean = {
    val sf: StructField = df.schema.apply(column)
    sf.dataType match {
      case d:StringType => StrUtil.isDateOrDateTimeType(sf)
      case _ => false
    }
  }

  /**
    * 判断是否是字符串类型
    * @param df
    * @param column
    * @return
    */
  def isStringType(df: DataFrame, column: String): Boolean = {
    val sf: StructField = df.schema.apply(column)
    sf.dataType match {
      case StringType => true
      case _ => false
    }
  }

  /**
    * 判断是否是Timestamp类型
    * @param df
    * @param column
    * @return
    */
  def isTimestampType(df: DataFrame, column: String): Boolean = {
    val sf: StructField = df.schema.apply(column)
    sf.dataType match {
      case TimestampType => true
      case _ => false
    }
  }

  /**
    * 判断是否是原子类型
    */
  def isAtomicType(df: DataFrame, column: String): Boolean = {
    val sf: StructField = df.schema.apply(column)
    sf.dataType match {
      case d: ArrayType => false
      case d: MapType => false
      case d: StructType => false
      case _ => true
    }
  }

  /**
    * 判断是否是数字类型
   */
  def isNumericType(df: DataFrame, column: String): Boolean = {
    val sf: StructField = df.schema.apply(column)
    sf.dataType match {
      case StringType => false
      case TimestampType => false
      case d: ArrayType => false
      case d: MapType => false
      case d: StructType => false
      case _ => true
    }
  }

  /**
    * 采样
    * @param df 原数据
//    * @param maxSampleCount 最大采样数量
    */
  def sample(df: DataFrame, sampleCount: Long): DataFrame = {
    val count = df.count()
    if(count > sampleCount) {
      val fraction = sampleCount*1.0/count
      //println(s"sampleFraction: $fraction, count: $count, maxSampleCount: $sampleCount")
      df.sample(false, fraction)
    }else {
      df
    }
  }

}
