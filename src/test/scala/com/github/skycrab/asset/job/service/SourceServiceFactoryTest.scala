package com.github.skycrab.asset.job.service

import com.github.skycrab.asset.job.bean.AssetException
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner

/**
  * Created by yihaibo on 2019-11-11.
  */
@RunWith(classOf[SpringRunner])
@SpringBootTest
class SourceServiceFactoryTest {

  @Autowired
  var sourceServiceFactory: SourceServiceFactory = _

  @Test
  def existSourceTest(): Unit = {
    val sourceService = sourceServiceFactory.getByName("hive")
    assertEquals(sourceService.name, "hive")

  }

  @Test(expected = classOf[AssetException])
  def notExistSourceTest(): Unit = {
    val sourceService = sourceServiceFactory.getByName("nnnnnn")
  }

}
