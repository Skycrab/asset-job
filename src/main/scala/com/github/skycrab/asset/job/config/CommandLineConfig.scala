package com.github.skycrab.asset.job.config

import java.io.File

import com.github.skycrab.asset.job.bean.InputParameter
import com.github.skycrab.asset.job.service.impl.TableSourceService
import com.github.skycrab.asset.job.util.HiveUtil
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

/**
  * Created by yihaibo on 2020-07-11.
  * 解析命令行参数
  */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class CommandLineConfig extends CommandLineRunner {

  @Autowired
  private var envConfig: EnvConfig = _

  @Autowired
  private var sceneConfig: SceneConfig = _

  @Autowired
  private var spark: SparkSession = _

  @Autowired
  private var sourceService: TableSourceService = _

  def getInputParameter: InputParameter = CommandLineConfig.inputParameter

  def getUserLabelScene: UserLabelScene = CommandLineConfig.userLabelScene

  override def run(args: String*): Unit = {
    println(s"mode: ${envConfig.mode} args:$args")

    val inputParameter = parseInputParameter(args: _*)
    CommandLineConfig.inputParameter = inputParameter
    CommandLineConfig.userLabelScene = loadScene(inputParameter.loadScene)
  }

  private def parseInputParameter(args: String*): InputParameter = {
    val inputParameter = if(envConfig.isDev) {
      devInputParameter()
    }else {
      InputParameter(args: _*)
    }

    println(s"parameter: $inputParameter")
    inputParameter
  }

  /**
    * 加载场景
    * @param loadScene
    * @return
    */
  private def loadScene(loadScene: Boolean): UserLabelScene = {
    if(!loadScene) {
      return UserLabelScene(List.empty)
    }

    if(envConfig.isDev) {
      initDevScene()
    }

    UserLabelScene(loadAllScene())
  }

  private def loadAllScene(): List[String] = {
    var df = sourceService.whereData(sceneConfig.getTableName, HiveUtil.partitionToMap(CommandLineConfig.inputParameter.partition))
    //获取所有场景
    val scenesCodes = df.select(sceneConfig.getScenePartitionColumn).distinct().collect().map(_.getString(0)).toList
    scenesCodes
  }

  /**
    * 默认测试环境命令行参数
    */
  private def devInputParameter(): InputParameter = {
    //加载测试数据，恢复分区
    val file = new File(classOf[CommandLineConfig].getClassLoader.getResource(CommandLineConfig.resourceName).getFile)
    val samplePath = file.getParent + "/sample/"
    val createSql = s"create table sample(uid long, age long, name string, last_login_time string, dt string) using csv options (path '$samplePath') partitioned by (dt)"
    spark.sql(createSql)
    spark.sql("ALTER TABLE sample RECOVER PARTITIONS")

    InputParameter("default.sample", "dt=2020-07-12", "hive", "[]", "[stability]", "true")
  }

  private def devInputParameter2(): InputParameter = {
    //加载测试数据，恢复分区
    val file = new File(classOf[CommandLineConfig].getClassLoader.getResource(CommandLineConfig.resourceName).getFile)
    val featureStatPath = file.getParent + "/featureStat/"
    val createSql = s"create table featureStat(db string,table string,feature string,useInPortrait bigint,last1DaysPortraitQueryCount bigint,last30DaysPortraitQueryCount bigint,last30DaysPortraitAvgQueryCount bigint,last1DaysOfflineQueryCount bigint,last30DaysOfflineQueryCount bigint,last30DaysOfflineAvgQueryCount bigint, dt string) using csv options (path '$featureStatPath') partitioned by (dt)"
    spark.sql(createSql)
    spark.sql("ALTER TABLE featureStat RECOVER PARTITIONS")

    InputParameter("default.sample", "dt=20200712", "hive", "[]", "[integrity]", "true")
  }

  /**
    * 加载测试环境场景
    */
  private def initDevScene(): Unit = {
    val file = new File(classOf[CommandLineConfig].getClassLoader.getResource(CommandLineConfig.resourceName).getFile)
    val scenePath = file.getParent + "/scene/"
    val createSql = s"create table dm_asset_scene(uid long,label long,feature_date string,role bigint,dt string,scene string) using csv options (path '$scenePath') partitioned by (dt, scene)"
    spark.sql(createSql)
    spark.sql("ALTER TABLE dm_asset_scene RECOVER PARTITIONS")
  }
}

case class UserLabelScene(scenesCodes: List[String])

object CommandLineConfig {

  private val resourceName: String = "application.properties"

  /**
    * 解析的命令行参数
    */
  private var inputParameter: InputParameter = _

  /**
    * 场景数据
    */
  private var userLabelScene: UserLabelScene = _

}

