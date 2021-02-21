package com.github.skycrab.asset.job.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

import scala.beans.BeanProperty

/**
  * Created by yihaibo on 2019-11-06.
  */
@ConfigurationProperties(prefix = "env")
@Component
class EnvConfig {

  @BeanProperty var mode: String = _

  /**
    * 是否是测试环境
    */
  def isDev: Boolean = {
    mode == "dev"
  }
}
