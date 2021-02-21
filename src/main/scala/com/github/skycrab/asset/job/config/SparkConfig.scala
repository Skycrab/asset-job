package com.github.skycrab.asset.job.config

import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.{Bean, Configuration}

/**
  * Created by yihaibo on 2019-11-06.
  */
@Configuration
class SparkConfig {
  @Autowired
  private var envConfig: EnvConfig = _

  @Bean
  def sparkSession(): SparkSession = {
    val builder = SparkSession
      .builder
    if(envConfig.isDev) {
      builder.master("local")
      builder.config("spark.driver.bindAddress", "127.0.0.1")
    }else {
      builder.enableHiveSupport()
    }
    builder.getOrCreate()
  }

}
