package com.github.skycrab.asset.job.common

import com.github.skycrab.asset.job.bean.AssetException
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent

import scala.collection.JavaConversions.mapAsScalaMap
import scala.reflect._

/**
  * Created by yihaibo on 2019-12-24.
  *
  * 当所有bean加载完执行
  */
class NamedFactory[T <: HasName : ClassTag] extends ApplicationListener[ContextRefreshedEvent] {
  private var nameMap: Map[String, T] = _

  override def onApplicationEvent(e: ContextRefreshedEvent): Unit = {
    if(e.getApplicationContext.getParent == null) {
      val context = e.getApplicationContext
      val beans = context.getBeansOfType(classTag[T].runtimeClass.asInstanceOf[Class[T]])
      nameMap = beans.map{case (_, v) => (v.name, v)}.toMap
    }
  }

  def getByName(name: String): T = {
    nameMap.getOrElse(name, throw new AssetException(s"无${name}服务，可用${nameMap.keys.toList}"))
  }

}
