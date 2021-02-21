package com.github.skycrab.asset.job.bean

import com.github.skycrab.asset.client.constants.MetricConstants
import org.apache.spark.sql.DataFrame

/**
  * Created by yihaibo on 2020-07-18.
  */

/**
  * @param sceneCode  场景coe
  * @param partition: 分区
  * @param dataFrame  场景数据
  * @param label label字段名称
  */
case class Scene(sceneCode: String, partition: String, dataFrame: DataFrame, label: String) {
  /**
    * 是否是主场景
    * @return
    */
  def isMainScene: Boolean = MetricConstants.SCENE_MAIN == sceneCode

}

