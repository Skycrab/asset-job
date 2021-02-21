package com.github.skycrab.asset.job.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

import scala.beans.BeanProperty

/**
  * Created by yihaibo on 2020-07-10.
  */
@ConfigurationProperties(prefix = "kafka")
@Component
class KafkaConfig {

  @BeanProperty var servers: String = _

  @BeanProperty var clusterId: String = _

  @BeanProperty var appId: String = _

  @BeanProperty var passwd: String = _

  @BeanProperty var groupId: String = _

  @BeanProperty var lingerMs: String = _

  @BeanProperty var batchSize: String = _

  @BeanProperty var topic: String = _

}
