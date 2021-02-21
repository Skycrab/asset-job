package com.github.skycrab.asset.job.handler

import com.github.skycrab.asset.client.metric.{Metric, MetricData}
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.config.CommandLineConfig
import com.github.skycrab.asset.job.service.MetricCollector
import com.github.skycrab.common.utils.JsonUtil
import org.springframework.beans.factory.annotation.Autowired

import scala.collection.mutable.ListBuffer

/**
  * Created by yihaibo on 2020-07-25.
  * 列指标处理器
  */
trait ColumnMetricHandler extends MetricHandler with Logging{

  @Autowired
  private var metricCollector: MetricCollector = _

  @Autowired
  private var cf: CommandLineConfig = _

  def handle(scene: Scene, column: String): Option[MetricData]

  def handle(scene: Scene): ListBuffer[Metric] = {
    val columns = scene.dataFrame.columns
    val excludeColumns = Array("uid", "role", "feature_date", "label")
    //提前收集，返回空
    columns.filter(column => !excludeColumns.contains(column)).foreach(column => {
      logInfo(s"process scene: ${scene.sceneCode} metricType: $subMetricType column: $column ")
      val metric = buildFieldMetric(column)
      metric.setSceneCode(scene.sceneCode)
      metric.setSubMetricType(subMetricType)
      val data = handle(scene, column)
      if(data.isDefined) {
        metric.setData(data.get)
        logInfo(s"metric: \n\n${JsonUtil.toJson(metric)} \n\n")
        metricCollector.collect(metric)
      }
    })
    ListBuffer.empty
  }

  private def buildFieldMetric(field: String): Metric = {
    val p = cf.getInputParameter

    val metric = new Metric()
    metric.setDb(p.db)
    metric.setTable(p.table)
    metric.setField(field)
    metric.setPartition(p.partition)

    metric
  }

}
