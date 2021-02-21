package com.github.skycrab.asset.job.handler

import com.github.skycrab.asset.client.constants.MetricConstants
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.config.CommandLineConfig
import com.github.skycrab.asset.job.service.MetricCollector
import com.github.skycrab.common.utils.JsonUtil
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.stereotype.Component

import scala.collection.JavaConversions.mapAsScalaMap

/**
  * Created by yihaibo on 2020-07-10.
  */
@Component
class MetricHandlerPipeLine extends ApplicationListener[ContextRefreshedEvent] with Logging {
  private var handlers = List[MetricHandler]()

  @Autowired
  private var metricCollector: MetricCollector = _

  @Autowired
  private var cf: CommandLineConfig = _

  @Autowired
  private var sceneLoader: SceneLoader = _

  override def onApplicationEvent(e: ContextRefreshedEvent): Unit = {
    if(e.getApplicationContext.getParent == null) {
      val hs = e.getApplicationContext.getBeansOfType(classOf[MetricHandler])
      handlers = hs.map(_._2).toList.sortBy(_.order)
    }
  }

  def handle(): Unit = {
    val hs = handlers.filter(h => cf.getInputParameter.subMetricTypes.contains(h.subMetricType))
    val scenes = loadScenes()

    scenes.foreach(scene => {
      hs.foreach(handler => {
        handleScene(scene, handler)
      })
    })
  }

  /**
    * 加载场景数据
    */
  private def loadScenes(): List[Scene] = {
    //表原始数据作为单独场景
    val sceneCodes = MetricConstants.SCENE_MAIN :: cf.getUserLabelScene.scenesCodes
    val partition = cf.getInputParameter.partition
    sceneCodes.map(sceneLoader.getScene(partition, _))
  }

  /**
    * 处理场景
    */
  private def handleScene(scene: Scene, handler: MetricHandler): Unit = {
    logInfo(s"process scene: ${scene.sceneCode} metricType: ${handler.subMetricType}")
    val metrics = handler.handle(scene)
    metrics.foreach(metric => {
      logInfo(s"metric: \n\n${JsonUtil.toJson(metric)} \n\n")
      metricCollector.collect(metric)
    })
  }
}

