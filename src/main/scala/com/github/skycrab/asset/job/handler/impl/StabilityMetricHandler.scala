package com.github.skycrab.asset.job.handler.impl

import com.github.skycrab.asset.client.enums.UpdateCycle._
import com.github.skycrab.asset.client.enums.{SubMetricType, UpdateCycle}
import com.github.skycrab.asset.client.metric.MetricData
import com.github.skycrab.asset.client.metric.quality.StabilityMetricData
import com.github.skycrab.asset.client.metric.quality.StabilityMetricData.Contrast
import com.github.skycrab.asset.job.bean.{AssetException, PsiBin, PsiBinGroup, Scene}
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.config.CommandLineConfig
import com.github.skycrab.asset.job.handler.{ColumnMetricHandler, SceneLoader}
import com.github.skycrab.asset.job.service.{CatalogServiceFactory, PsiService}
import com.github.skycrab.asset.job.util._
import org.apache.commons.beanutils.BeanUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.JavaConverters._

/**
  * Created by yihaibo on 2020-07-14.
  * 稳定性处理器
  */
@Component
class StabilityMetricHandler extends ColumnMetricHandler with Logging {
  @Autowired
  private var cf: CommandLineConfig = _

  @Autowired
  private var catalogServiceFactory: CatalogServiceFactory = _

  @Autowired
  private var sceneLoader: SceneLoader = _

  @Autowired
  private var psiService: PsiService = _

  /**
    * 子维度类型
    * @return
    */
  override def subMetricType: SubMetricType = SubMetricType.STABILITY

  override def order = 3

  override def handle(scene: Scene, column: String): Option[MetricData] = {
    val data = new StabilityMetricData()
    //现只支持数字类型
    if (!(DataFrameUtil.isNumericType(scene.dataFrame, column)
          || MetricUtil.preditIsDateType(scene.dataFrame, column))) {
      return None
    }

    val partition = cf.getInputParameter.partition
    val tableName = cf.getInputParameter.tableName
    val mode = cf.getInputParameter.mode

    val partitions = catalogServiceFactory.getByName(mode).partitions(tableName)
    val index = partitions.indexOf(partition)

    if (index == -1) {
      throw new AssetException(s"$tableName 没有分区 $partition")
    }

    val updateCycle = predictUpdateCycle(partitions)
    data.setUpdateCycle(updateCycle)

    trainSetPartitionIntervals(updateCycle).foreach { case (interval, attr) =>
      if (index >= interval) {
        val trainPartition = partitions(index - interval)
        logInfo(s"trainPartition: $trainPartition")
        val trainScene = sceneLoader.getScene(trainPartition, scene.sceneCode)
        calPsi(trainScene, scene, column, attr, data)
      }
    }

    Some(data)
  }

  /**
    * 根据分区预测更新周期： 天更、周更、月更
    *
    * @param partitions [year=2020/month=07/day=05,year=2020/month=07/day=12,year=2020/month=07/day=19]
    * @return
    */
  private def predictUpdateCycle(partitions: Array[String]): UpdateCycle = {
    val length = partitions.length

    //分区列表中只有一条数据时认为是周更
    if(length <= 1) return WEEK
    //如果分区字段不是约定时间字段则返回默认周更
    if(!isTimePatitionColumn(HiveUtil.partitionToMap(partitions(length-1)))) return WEEK

    //分区中最后两条数据做样本数据比对计算天数
    val last1stPatition = StrUtil.getPatitonToEpochday(partitions(length-1))
    val last2ndPatition = StrUtil.getPatitonToEpochday(partitions(length-2))
    val diff = math.abs(last1stPatition-last2ndPatition)

    if (diff == 7) {
      WEEK
    } else if(diff == 1) {
      DAY
    } else{
      MONTH
    }
  }

  private def isTimePatitionColumn(lastOnePatitionMap: Map[String, String]) = {
    val timePatitionColumn = Set("pt", "dt", "year", "month", "day")
    val isTimePatitionColumn = lastOnePatitionMap.keySet.intersect(timePatitionColumn).nonEmpty
    isTimePatitionColumn
  }



  /**
    * 根据周期类型返回psi训练集计算分区
    */
  private def trainSetPartitionIntervals(updateCycle: UpdateCycle): Array[(Int, String)] = {
    updateCycle match {
      case DAY => Array((1, "1Days"), (7, "7Days"), (30, "30Days"), (180, "180Days"))
      case WEEK => Array((1, "7Days"), (4, "30Days"), (25, "180Days"))
      case MONTH => Array((1, "30Days"), (6, "180Days"))
    }
  }

  private def calPsi(trainScene: Scene, testScene: Scene, column: String, attr: String, data: StabilityMetricData): Unit = {
    val psiBinGroup = psiService.calPsi(trainScene.dataFrame, testScene.dataFrame, column)
    logInfo(s"psiBinGroup, $psiBinGroup")

    BeanUtils.copyProperty(data, s"last${attr}Psi", psiBinGroup.psi)

    val grups = psiBinGroup.bins.map(bin => {
      StabilityMetricData.Group.builder()
        .label(getLabel(bin, psiBinGroup))
        .trainCount(bin.trainCount)
        .trainTotal(bin.trainTotal)
        .trainProportion(NumberUtil.toPrecision(bin.trainPercent))
        .testCount(bin.testCount)
        .testTotal(bin.testTotal)
        .testProportion(NumberUtil.toPrecision(bin.testPercent))
        .build()
    }).toList.asJava

    val contrast = Contrast.builder().trainPartition(trainScene.partition)
      .testPartition(testScene.partition)
      .groups(grups)
      .build()

    BeanUtils.copyProperty(data, s"last${attr}PsiContrast", contrast)
  }

  private def getLabel(psiBin: PsiBin, psiBinGroup: PsiBinGroup): String = {
    if (psiBin.isUniqueBin) {
      MetricUtil.getUniqueLabel(psiBin.binIndex, psiBinGroup.uniqueLabels)
    } else {
      MetricUtil.getQuantileLabel(psiBin.binIndex, psiBinGroup.labels)
    }
  }
}
