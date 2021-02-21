package com.github.skycrab.asset.job.service

import com.github.skycrab.asset.job.common.HasName
import org.apache.spark.sql.DataFrame

/**
  * Created by yihaibo on 2019-11-08.
  * 数据源服务
  */
trait SourceService extends HasName {
  /**
    * 获取where条件数据
    * @param where Map{"year": 2019, "month": 10, "day": 10}
    * @param ignoreWhereColumn 是否排除where字段
    */
  def whereData(tableName: String, where: Map[String, _], ignoreWhereColumn: Boolean = true): DataFrame
}
