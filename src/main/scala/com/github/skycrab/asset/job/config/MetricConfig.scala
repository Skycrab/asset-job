package com.github.skycrab.asset.job.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

import scala.beans.BeanProperty

/**
  * Created by yihaibo on 2020-07-15.
  */
@ConfigurationProperties(prefix = "metric")
@Component
class MetricConfig {
  /**
    * 采样数量
    */
  @BeanProperty var sampleCount: Long = _

  /**
    * 分箱数量
    */
  @BeanProperty var discretizeNumBucket: Int = _


  /**
    * 数字类型缺失值
    */
  @BeanProperty var numberMissingValue: Array[Double] = _


}
