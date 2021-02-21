package com.github.skycrab.asset.job.service.impl

import com.github.skycrab.asset.job.service.MetricCollector
import org.springframework.stereotype.Service

/**
  * Created by yihaibo on 2020-07-10.
  */
@Service
class MysqlMetricCollector extends MetricCollector {

  /**
    * 指标收集
    */
  override def collect(data: Object): Unit = {

  }

}
