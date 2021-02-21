package com.github.skycrab.asset.job.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

import scala.beans.BeanProperty

/**
  * Created by yihaibo on 2020-08-06.
  */
@ConfigurationProperties(prefix = "scene")
@Component
class SceneConfig {

  @BeanProperty var tableName: String = _

  @BeanProperty var scenePartitionColumn: String = _

  @BeanProperty var featureDateColumn: String = _

  @BeanProperty var labelColumn: String = _

  @BeanProperty var roleColumn: String = _


}
