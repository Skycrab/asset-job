package com.github.skycrab.asset.job.handler.impl

import com.github.skycrab.asset.client.enums.SubMetricType
import com.github.skycrab.asset.client.metric.Metric
import com.github.skycrab.asset.client.metric.application.OfflineQueryMetricData
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.config.CommandLineConfig
import com.github.skycrab.asset.job.handler.MetricHandler
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{avg, col}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.mutable.ListBuffer

/**
  * Created by yihaibo on 2020-07-25.
  * 离线特征使用统计
  */
@Component
class OfflineQueryMetricHandler extends MetricHandler with Logging {

  @Autowired
  private var commandLineConfig: CommandLineConfig = _

  private val mustColumns = Array("db", "tablename", "featurename", "last1daysofflinequerycount", "last30daysofflinequerycount",
    "last30daysofflineavgquerycount")

  /**
    * 子维度类型
    */
  override def subMetricType: SubMetricType = SubMetricType.OFFLINE_QUERY

  override def handle(scene: Scene): ListBuffer[Metric] = {
    if(!canHandler(scene)) return ListBuffer.empty

    //计算 近30天所有特征平均查询次数
    val last30DaysOfflineAvgAllQueryCount = scene.dataFrame
      .filter(col("last30daysofflinequerycount")>0)
      .agg(avg("last30daysofflinequerycount")).collect()(0).getDouble(0).toLong

    val data = scene.dataFrame.select(mustColumns.head, mustColumns.tail: _*).collect()
    val result = ListBuffer[Metric]()
    data.foreach(row => result.append(buildMetric(scene, row, last30DaysOfflineAvgAllQueryCount)))

    result
  }

  /**
    * 场景数据是否可处理，应用维度只处理特定表数据
    */
  private def canHandler(scene: Scene): Boolean = {
    val columns = scene.dataFrame.columns
    val columnsAllExist = mustColumns.forall(columns.indexOf(_) != -1)

    if(scene.isMainScene && columnsAllExist) {
      true
    }else {
      false
    }
  }

  private def buildMetric(scene: Scene, row: Row, last30DaysOfflineAvgAllQueryCount: Long): Metric = {
    val metric = new Metric()
    metric.setDb(row.getAs[String]("db"))
    metric.setTable(row.getAs[String]("tablename"))
    metric.setField(row.getAs[String]("featurename"))
    metric.setSceneCode(scene.sceneCode)
    metric.setSubMetricType(subMetricType)
    metric.setPartition(commandLineConfig.getInputParameter.partition)
    val metricData = new OfflineQueryMetricData()
    metricData.setLast1DaysOfflineQueryCount(row.getAs[Long]("last1daysofflinequerycount"))
    metricData.setLast30DaysOfflineQueryCount(row.getAs[Long]("last30daysofflinequerycount"))
    metricData.setLast30DaysOfflineAvgQueryCount(row.getAs[Long]("last30daysofflineavgquerycount"))
    metricData.setLast30DaysOfflineAvgAllQueryCount(last30DaysOfflineAvgAllQueryCount)
    metric.setData(metricData)

    metric
  }
}
