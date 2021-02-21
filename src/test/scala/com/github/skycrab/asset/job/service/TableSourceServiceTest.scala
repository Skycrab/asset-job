package com.github.skycrab.asset.job.service

import com.github.skycrab.asset.job.service.impl.TableSourceService
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

/**
  * Created by yihaibo on 2019-11-08.
  */
@RunWith(classOf[SpringRunner])
@SpringBootTest
class TableSourceServiceTest extends SparkServiceTestBase {
  @Autowired
  var tableSourceService: TableSourceService = _


  @Test
  def whereDataTest(): Unit = {
    val df2 = tableSourceService.whereData("person", Map("name" -> "Bob"))
    assertEquals(df2.count(), 1L)
  }

}
