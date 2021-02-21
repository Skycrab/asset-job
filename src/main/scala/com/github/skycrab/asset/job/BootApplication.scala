package com.github.skycrab.asset.job

import com.github.skycrab.asset.job.handler.MetricHandlerPipeLine
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.boot.{CommandLineRunner, SpringApplication}

/**
 * Created by yihaibo on 2019-11-05.
 */

object BootApplication  {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[Monitor], args:_*)
  }

}

/**
  * 部分版本自动配置依赖版本过高，spark自带版本低，如无需使用，可exclude
  */
@SpringBootApplication
@EnableAutoConfiguration(exclude = Array(classOf[GsonAutoConfiguration], classOf[RestClientAutoConfiguration], classOf[KafkaAutoConfiguration]))
class Monitor extends CommandLineRunner {

  @Autowired
  private var handlerPipeline: MetricHandlerPipeLine = _

  override def run(args: String*): Unit = {
    handlerPipeline.handle()
  }

}
