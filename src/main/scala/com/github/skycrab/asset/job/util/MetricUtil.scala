package com.github.skycrab.asset.job.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

/**
  * Created by yihaibo on 2020-07-19.
  */
object MetricUtil {

  /**
    * 获取分位数显示label(左闭右开)
    */
  def getQuantileLabel(index: Int, labels: Array[_]): String = {
    val maxLabelCount = labels.length -2
    if(index == 0) {
      s"[-∞, ${labels(index+1)})"
    }else if(index == maxLabelCount) {
      s"[${labels(index)}, +∞)"
    }else {
      s"[${labels(index)}, ${labels(index+1)})"
    }
  }

  /**
    * 采样判断列是否是日期类型，日期类型比率大于0.1
    */
   def preditIsDateType(df: DataFrame, column: String): Boolean = {
    val notNullAndNotEmptyDf = df.filter(col(column).isNotNull)
      .select(col(column).cast(StringType))
      .filter(col(column).notEqual(""))

    val data: Array[String] = notNullAndNotEmptyDf.take(200).map(_.getAs[String](column))
    val dateTypeCount = data.map(d => if(StrUtil.isDateOrDateTimeType(d)) 1 else 0).sum

    val dateTypeProportion = NumberUtil.proportion(dateTypeCount, data.length)
    dateTypeProportion > 0.1
  }

  /**
    * 获取分位数显示label(左闭右开)
    */
  def getQuantileLabel(indexs: List[Int], labels: Array[_]): String = {
    val maxLabelCount = labels.length -2

    if(indexs == null) {
      "NULL"
    }else if(indexs.size == 1) {
      getQuantileLabel(indexs.head, labels)
    }else {
      val start = indexs.head
      val end = indexs.last
      val left = if(start == 0) "[-∞" else s"[${labels(start)}"
      val right = if(end == maxLabelCount) "+∞)" else s"${labels(end+1)})"
      left + ", " + right
    }
  }

  /**
    * 获取单独箱显示label(左开右开)
    */
  def getUniqueLabel(index: Int, labels: Array[_]): String = {
    if(index == -1) {
      "NULL"
    }else {
      s"${labels(index)}"
    }
  }

  /**
    * 获取单独箱显示label(左开右开)
    */
  def getUniqueLabel(indexs: List[Int], labels: Array[_]): String = {
    if(indexs == null) {
      "NULL"
    }else {
      indexs.map(labels(_)).mkString(", ")
    }
  }

}
