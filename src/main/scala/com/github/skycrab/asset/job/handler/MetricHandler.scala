package com.github.skycrab.asset.job.handler

import com.github.skycrab.asset.client.enums.SubMetricType
import com.github.skycrab.asset.client.metric.Metric
import com.github.skycrab.asset.job.bean.Scene

import scala.collection.mutable.ListBuffer

/**
  * Created by yihaibo on 2020-07-10.
  */
trait MetricHandler {

  /**
    * 优先级，越小越优先
    */
  def order: Int = 9

  /**
    * 子维度类型
    * @return
    */
  def subMetricType: SubMetricType

  def handle(scene: Scene): ListBuffer[Metric]

}
