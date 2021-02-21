package com.github.skycrab.asset.job.config

import java.util

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.{DefaultKafkaProducerFactory, KafkaTemplate}

/**
  * Created by yihaibo on 2020-07-10.
  */
//@Configuration
//@EnableKafka
class KafkaRuntimeConfig {

  @Autowired
  private var kafkaConfig: KafkaConfig = _

  @Bean def kafkaTemplate = new KafkaTemplate[String, String](producerFactory)

  @Bean def producerFactory = new DefaultKafkaProducerFactory[String, String](producerConfigs)

  @Bean
  def producerConfigs: util.Map[String, AnyRef] = {
    val props = new util.HashMap[String, AnyRef]
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getServers)
    //leader将消息记录写入到本地
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaConfig.getBatchSize)
    props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaConfig.getLingerMs)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put("sasl.mechanism", "PLAIN")
    props.put("security.protocol", "SASL_PLAINTEXT")
    val format = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s.%s\" password=\"%s\";"
    val jaasConfig = String.format(format, kafkaConfig.getClusterId, kafkaConfig.getAppId, kafkaConfig.getPasswd)
    props.put("sasl.jaas.config", jaasConfig)
    props
  }


}
