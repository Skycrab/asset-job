package com.github.skycrab.asset.job.service

/**
  * 指标收集器
  * Created by yihaibo on 2020-07-10.
  */
trait MetricCollector {
  /**
    * 指标收集
    */
  def collect(data: Object): Unit


  def collect(datas: List[Object]): Unit = {
    datas.foreach(collect)
  }

}
