package com.github.skycrab.asset.job.service.impl

import com.github.skycrab.asset.job.service.SourceService
import com.github.skycrab.asset.job.util.StrUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
  * Created by yihaibo on 2019-11-08.
  * 直接从hive表中获取数据源
  */
@Service("hiveSourceService")
class TableSourceService extends SourceService {


  override def name: String = "hive"

  @Autowired
  private var spark: SparkSession = _

  /**
    * 获取where条件数据
    * @param where Map{"year": 2019, "month": 10, "day": 10}
    * @param ignoreWhereColumn 是否排除分区字段
    */
  override def whereData(tableName: String, where: Map[String, _], ignoreWhereColumn: Boolean = true): DataFrame = {
    val sql = s"select * from ${tableName} ${StrUtil.where(where)}"
    var df = spark.sql(sql)
    if(ignoreWhereColumn) {
      df = df.drop(where.keys.toList :_*)
    }

    df
  }
}
