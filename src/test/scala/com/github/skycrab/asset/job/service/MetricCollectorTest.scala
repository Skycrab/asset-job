package com.github.skycrab.asset.job.service

import com.github.skycrab.asset.client.metric.Metric
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

/**
  * Created by yihaibo on 2020-07-10.
  */
@RunWith(classOf[SpringRunner])
@SpringBootTest
class MetricCollectorTest {

  @Autowired
  var metricCollector: MetricCollector = _

  @Test
  def collectTest(): Unit = {
    val metric = new Metric()
    metric.setDb("db")
    metric.setTable("table")
    metricCollector.collect(metric)

  }
}
