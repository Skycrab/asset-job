package com.github.skycrab.asset.job.util

import com.google.common.base.Splitter
import scala.collection.JavaConverters._

/**
  * Created by yihaibo on 2020-07-10.
  */
object HiveUtil {

  /**
    * 分区字符串转化为Map
    * @param partition year=2020/month=08/day=01
    * @return
    */
  def partitionToMap(partition: String): Map[String, String] = {
    Splitter.on('/').trimResults.omitEmptyStrings().withKeyValueSeparator("=").split(partition).asScala.toMap
  }

  /**
    * 分区字符串分区列表
    * @param partition year=2020/month=08/day=01
    * @return [year,month,day]
    */
  def partitionKeys(partition: String): List[String] = {
    Splitter.on('/').trimResults.omitEmptyStrings().withKeyValueSeparator("=").split(partition).keySet().asScala.toList
  }

}
