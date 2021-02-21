package com.github.skycrab.asset.job.service.impl

import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.service.CatalogService
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import scala.collection.mutable

/**
  * Created by yihaibo on 2019-11-08.
  */
@Service
class HiveCatalogServiceImpl extends CatalogService with Logging{

  override def name: String = "hive"

  @Autowired
  private var spark: SparkSession = _

  private val partitionCache = mutable.HashMap[String, Array[String]]()

  /**
    * 获取分区列
    *
    * @param tableName 库表名
    * @return
    */
  override def partitionColumns(tableName: String): Array[String] = {
    spark.catalog.listColumns(tableName).filter(_.isPartition).collect().map(_.name)
  }

  /**
    * 获取分区信息
    *
    * year=2019/month=09/day=13 -> Map()
    * @param tableName
    * @return
    */
  override def partitions(tableName: String): Array[String] = {
    partitionCache.getOrElseUpdate(tableName, queryPartitions(tableName))
  }

  private def queryPartitions(tableName: String): Array[String] = {
    logInfo(s"queryPartitions $tableName")
    val df = spark.sql(s"show partitions ${tableName}")
    df.collect().map(_.getString(0))
  }

  /**
    * 判断是否存在某一分区
    *
    * show partitions db.table partition(year='2020',day='09',month='07');
    * @param partitionMap
    * @return
    */
  override def existPartition(tableName: String, partitionMap: Map[String, String]): Boolean = {
    val s = for((k, v) <- partitionMap) yield s"${k}='${v}'"
    val partition = s.mkString(",")
    val df = spark.sql(s"show partitions $tableName partition($partition)")
    df.collect().length > 0
  }

}
