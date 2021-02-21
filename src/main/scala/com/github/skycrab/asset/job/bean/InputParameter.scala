package com.github.skycrab.asset.job.bean

import com.github.skycrab.asset.client.enums.SubMetricType
import com.github.skycrab.asset.job.util.HiveUtil
import com.google.common.base.{CharMatcher, Splitter}

import scala.collection.JavaConverters._

/**
  * Created by yihaibo on 2019-11-07.
  *
  * @param tableName  表名
  * @param partition  分区 year=2020/month=08/day=01
  * @param mode  表模式(hive)
  * @param columns 需计算列，[]代表计算所有
  * @param subMetricTypes 需计算子维度类型，[]代表所有
  */
case class InputParameter(tableName:String, partition: String, mode: String, columns: List[String], subMetricTypes: List[SubMetricType], loadScene: Boolean) {

  /**
    * 针对parquet单独处理
    */
  def db: String = {
    tableName.split("\\.")(0)
  }

  def table: String =  {
    tableName.split("\\.")(1)
  }

  def partitionMap(): Map[String, String] = HiveUtil.partitionToMap(partition)

}


object InputParameter {
  private val modes = List("hive", "parquet")

  def apply(args: String*): InputParameter = {
    if(args.length != 6) {
      throw new AssetException("parameter error, tableName partition hive columns subMetricTypes loadScene")
    }

    val tableName = args(0)
    if(tableName.split("\\.").length !=2) {
      throw new AssetException(s"parameter error, tableName must db.table")
    }

    // year=2020/month=08/day=01
    val partition = args(1)

    val mode = args(2)
    if(modes.indexOf(mode) == -1) {
      throw new AssetException(s"parameter error, mode must in ${modes}")
    }

    val fields = parseColumns(args(3))

    val subMetricType = parseSubMetricTypes(args(4))

    val loadScene = args(5).toBoolean

    InputParameter(tableName, partition, mode, fields, subMetricType, loadScene)
  }


  private def parseColumns(columns: String): List[String] = {
    Splitter.on(",").omitEmptyStrings().trimResults(CharMatcher.anyOf("[]")).split(columns).asScala.toList
  }

  /**
    * 解析子类型，[]为所有子类型
    * @param subMetricTypes 子类型列表，逗号分隔
    * @return
    */
  private def parseSubMetricTypes(subMetricTypes: String): List[SubMetricType] = {
    val metricTypes = Splitter.on(",").omitEmptyStrings().trimResults(CharMatcher.anyOf("[]")).split(subMetricTypes).asScala.toList
    if(metricTypes.isEmpty) {
      SubMetricType.values().toList
    }else {
      metricTypes.map(SubMetricType.of)
    }
  }
}
