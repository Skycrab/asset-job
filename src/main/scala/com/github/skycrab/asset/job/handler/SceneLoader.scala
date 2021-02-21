package com.github.skycrab.asset.job.handler

import com.github.skycrab.asset.client.constants.MetricConstants
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.service.impl.TableSourceService
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.config.{CommandLineConfig, SceneConfig}
import com.github.skycrab.asset.job.service.SourceServiceFactory
import com.github.skycrab.asset.job.util.HiveUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.mutable

/**
  * Created by yihaibo on 2020-07-18.
  */
@Component
class SceneLoader extends Logging {

  @Autowired
  private var cf: CommandLineConfig = _

  @Autowired
  private var sceneConfig: SceneConfig = _

  @Autowired
  private var sourceService: TableSourceService = _

  @Autowired
  private var sourceServiceFactory: SourceServiceFactory = _

  private val sceneCache = mutable.HashMap[String, Scene]()
  private val inputCache = mutable.HashMap[String, DataFrame]()

  def getScene(partition: String, sceneCode: String): Scene = {
    val key = s"$partition#$sceneCode"
    sceneCache.getOrElseUpdate(key, loadScene(partition, sceneCode))
  }

  /**
    * 加载场景数据
    *
    * @param partition 分区
    * @param sceneCode 场景code
    * @return
    */
  private def loadScene(partition: String, sceneCode: String): Scene = {
    logInfo(s"loadScene $partition, $sceneCode")
    if (MetricConstants.SCENE_MAIN == sceneCode) {
      loadMainScene(partition, sceneCode)
    } else {
      loadLabelScene(partition, sceneCode)
    }
  }

  /**
    * 加载主场景
    * SCENE_MAIN -> 主场景，全量用户
    */
  private def loadMainScene(partition: String, sceneCode: String): Scene = {
    val input = getInputTableData(partition)
    input.cache()
    Scene(sceneCode, partition, input, "")
  }

  /**
    * 加载Label场景 -> 场景数据与主数据关联
    */
  private def loadLabelScene(partition: String, sceneCode: String): Scene = {
    val scene = loadSceneByPatition(partition, sceneCode)
    //获取所需特征最大最小关联时间
    val row = scene.dataFrame.agg(min(sceneConfig.getFeatureDateColumn), max(sceneConfig.getFeatureDateColumn)).collect().head
    val minDate = row.getString(0)
    val maxDate = row.getString(1)
    val input = loadMultDateInputTableData(minDate, maxDate)
    val joinDf = scene.dataFrame.join(input, Seq(MetricConstants.UID_FIELD, sceneConfig.getFeatureDateColumn), "left")
    joinDf.explain()
    joinDf.cache()
    Scene(sceneCode, partition, joinDf, scene.label)
  }

  private def loadSceneByPatition(partition: String, sceneCode: String): Scene = {
    var df = sourceService.whereData(sceneConfig.getTableName, HiveUtil.partitionToMap(partition)).filter(s"${sceneConfig.getScenePartitionColumn}='$sceneCode'")
    tableRole().foreach(rule => df = df.filter(s"${sceneConfig.getRoleColumn}=$rule"))
    df.explain()
    Scene(sceneCode, partition, df, sceneConfig.getLabelColumn)
  }

  /**
    * 获取表对应的角色，场景区分乘客和司机
    *
    * @return
    */
  private def tableRole(): Option[Int] = {
    val tableName = cf.getInputParameter.tableName
    if (tableName.contains("_passenger_")) {
      Some(1)
    } else if (tableName.contains("_driver_")) {
      Some(2)
    } else {
      None
    }
  }

  def getInputTableData(partition: String): DataFrame = {
    inputCache.getOrElseUpdate(partition, loadInputTableData(partition))
  }

  /**
    * 加载输入表数据
    */
  private def loadInputTableData(partition: String): DataFrame = {
    logInfo(s"loadInputTableData, partition:$partition")
    val p = cf.getInputParameter

    val sourceDf = sourceServiceFactory.getByName(p.mode).whereData(p.tableName, HiveUtil.partitionToMap(partition))
    val columns = if (p.columns.isEmpty) sourceDf.columns.toList else p.columns
    val df = sourceDf.select(columns.head, columns.tail: _*)
    df.printSchema()
    df
  }

  /**
    * 加载多日期表数据，防止label泄露，使用业务时间特征数据
    */
  private def loadMultDateInputTableData(minDate: String, maxDate: String): DataFrame = {
    logInfo(s"loadMultDateInputTableData, minDate:$minDate, maxDate:$maxDate")
    val p = cf.getInputParameter
    val sourceDf = sourceServiceFactory.getByName(p.mode).whereData(p.tableName, Map.empty)
    val partitionKeys = HiveUtil.partitionKeys(p.partition)
    val partiitonColumns = partitionKeys.map(col(_))

    //排除分区列
    val selectColumns = if (p.columns.isEmpty) sourceDf.columns.filter(partitionKeys.indexOf(_) == -1).toList else p.columns
    val df = sourceDf.withColumn(sceneConfig.getFeatureDateColumn, concat_ws("-", partiitonColumns: _*))
      .filter(concat_ws("-", partiitonColumns: _*).between(minDate, maxDate))
      .select(sceneConfig.getFeatureDateColumn, selectColumns: _*)
      .where(date_format(concat_ws("-", partiitonColumns: _*), "u") === 7)
    df.printSchema()
    df
  }

}


