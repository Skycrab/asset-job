package com.github.skycrab.asset.job.service

import com.github.skycrab.asset.job.common.HasName

/**
  * Created by yihaibo on 2019-11-08.
  */
trait CatalogService extends HasName{

  /**
    * 获取分区列
    * @param tableName 库表名
    * @return
    */
  def partitionColumns(tableName: String): Array[String]


  /**
    * 获取分区信息
    * @param tableName
    * @return
    */
  def partitions(tableName: String): Array[String]


  /**
    * 判断是否存在某一分区
    * @param partitionMap
    * @return
    */
  def existPartition(tableName: String, partitionMap: Map[String, String]): Boolean


}
