package com.github.skycrab.asset.job.util

import org.springframework.context.{ApplicationContext, ApplicationContextAware}
import org.springframework.stereotype.Component

/**
  * Created by yihaibo on 2020-07-10.
  */
@Component
class SpringContextUtils extends ApplicationContextAware {

  override def setApplicationContext(applicationContext: ApplicationContext): Unit = {
    SpringContextUtils.applicationContext = applicationContext
  }
}


object SpringContextUtils {
  private var applicationContext: ApplicationContext =_

  def getApplicationContext: ApplicationContext = {
    applicationContext
  }

}
