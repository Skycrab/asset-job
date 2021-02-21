package com.github.skycrab.asset.job.service.impl

import com.github.skycrab.asset.job.config.KafkaConfig
import com.github.skycrab.asset.job.service.MetricCollector
import com.github.skycrab.common.utils.JsonUtil
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate

/**
  * Created by yihaibo on 2020-07-10.
  */
//@Service
class KafkaMetricCollector extends MetricCollector {

  @Autowired
  private var kafkaTemplate: KafkaTemplate[String, String] = _

  @Autowired
  private var kafkaConfig: KafkaConfig = _

  /**
    * 指标收集
    */
  override def collect(data: Object): Unit = {
    kafkaTemplate.send(kafkaConfig.getTopic, JsonUtil.toJson(data))
  }

}
