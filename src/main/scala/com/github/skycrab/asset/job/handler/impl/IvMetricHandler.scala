package com.github.skycrab.asset.job.handler.impl

import java.time.LocalDate

import com.github.skycrab.asset.client.constants.MetricConstants
import com.github.skycrab.asset.client.enums.SubMetricType
import com.github.skycrab.asset.client.metric.MetricData
import com.github.skycrab.asset.client.metric.importance.IvMetricData
import com.github.skycrab.asset.client.metric.importance.IvMetricData.Group
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.config.MetricConfig
import com.github.skycrab.asset.job.handler.ColumnMetricHandler
import com.github.skycrab.asset.job.util.{DataFrameUtil, MetricUtil, NumberUtil}
import org.apache.spark.ml.feature.{WoeBinning, WoeBinningModel, WoeIv, WoeIvGroup}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.JavaConverters._

/**
  * Created by yihaibo on 2020-07-20.
  * iv值处理器
  */
@Component
class IvMetricHandler extends ColumnMetricHandler with Logging {

  @Autowired
  private var metricConfig: MetricConfig = _
  /**
    * 子维度类型
    */
  override def subMetricType: SubMetricType = SubMetricType.IV

  override def order = 2

  override def handle(scene: Scene, column: String): Option[MetricData] = {
    if (scene.sceneCode == MetricConstants.SCENE_MAIN) return None

    val data = new IvMetricData()
    val df = scene.dataFrame

    if(DataFrameUtil.isNumericType(scene.dataFrame, column)){
      calNumericTypeComputation(data, df, column)
    }else if(MetricUtil.preditIsDateType(scene.dataFrame, column)){
      calDateTypeIVComputation(data, df, column)
    }else{
      None
    }
  }

  private def calDateTypeIVComputation(data: IvMetricData, df: DataFrame, column: String): Option[IvMetricData] = {
    val noMissingValueDf = df.select(datediff(col(column).substr(0, 10).cast(DateType), to_date(lit("1970-01-01"), "yyyy-MM-dd")).cast(DoubleType).as(column), col("label"))
      .filter(col(column).notEqual(0) || col(column).isNull)
    val (isContinuous: Boolean, woeModel: WoeBinningModel, ivOption: Option[WoeIvGroup]) = calIVComputationCommon(data, noMissingValueDf, column)
    if (ivOption.isDefined) {
      val dateLabels = woeModel.labelWoeIv.label.map(item => if(item != null) LocalDate.of(1970, 1, 1).plusDays(item.toString.toDouble.toInt) else null)
      buildData(ivOption.get, dateLabels, isContinuous, data)
      Some(data)
    } else {
      None
    }
  }

  private def calNumericTypeComputation(data: IvMetricData, df: DataFrame, column: String): Option[IvMetricData] = {
    val (isContinuous: Boolean, woeModel: WoeBinningModel, ivOption: Option[WoeIvGroup]) = calIVComputationCommon(data, df, column)
    if (ivOption.isDefined) {
      buildData(ivOption.get, woeModel.labelWoeIv.label, isContinuous, data)
      Some(data)
    } else {
      None
    }
  }

  private def calIVComputationCommon(data: IvMetricData, df: DataFrame, column: String): (Any, WoeBinningModel, Option[WoeIvGroup]) = {
    val uniqueCount = df.select(col(column)).distinct().count()
    //是否是连续变量
    val isContinuous = uniqueCount > 50
    data.setIsDiscreteVariable(!isContinuous)

    val woeBinning = new WoeBinning()
      .setInputCol(column)
      .setOutputCol(s"${column}_woe")
      .setLabelCol("label")
      .setContinuous(isContinuous)

    val woeModel = woeBinning.fit(df)
    val label = woeModel.labelWoeIv.label
    val ivOption = woeModel.labelWoeIv.woeIvGroup

    logInfo(s"$column label: ${label.toList}) woe:$ivOption")
    (isContinuous, woeModel, ivOption)
  }

  private def buildData(woeIvGroup: WoeIvGroup, lables: Array[_], isContinuous: Boolean, data: IvMetricData): Unit = {
    data.setIv(woeIvGroup.iv)
    val groups = woeIvGroup.woeIvs.map(toGroup(_, lables, isContinuous)).asJava
    data.setGroups(groups)
  }

  private def toGroup(woeIv: WoeIv, lables: Array[_], isContinuous: Boolean): Group = {
    val label = if(isContinuous) {
      MetricUtil.getQuantileLabel(woeIv.position, lables)
    }else {
      MetricUtil.getUniqueLabel(woeIv.position, lables)
    }

    Group.builder()
      .label(label)
      .badCount(woeIv.badCnt)
      .goodCount(woeIv.goodCnt)
      .badTotal(woeIv.badTotal)
      .goodTotal(woeIv.goodTotal)
      .marginBadRate(NumberUtil.toPrecision(woeIv.marginBadRate))
      .marginGoodRate(NumberUtil.toPrecision(woeIv.marginGoodRate))
      .woe(NumberUtil.toPrecision(woeIv.woe))
      .iv(NumberUtil.toPrecision(woeIv.iv))
      .build()
  }

}
