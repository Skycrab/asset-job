package com.github.skycrab.asset.job.handler.impl

import com.github.skycrab.asset.client.constants.MetricConstants
import com.github.skycrab.asset.client.enums.SubMetricType
import com.github.skycrab.asset.client.metric.MetricData
import com.github.skycrab.asset.client.metric.importance.CorrMetricData
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.handler.ColumnMetricHandler
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.config.MetricConfig
import com.github.skycrab.asset.job.util.{DataFrameUtil, MetricUtil}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType}
import org.apache.spark.sql.{Dataset, Row}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

/**
  * Created by yihaibo on 2020-07-22.
  * 相关系数
  */
@Component
class CorrMetricHandler extends ColumnMetricHandler with Logging {

  @Autowired
  private var metricConfig: MetricConfig = _

  /**
    * 子维度类型
    */
  override def subMetricType: SubMetricType = SubMetricType.CORR

  override def order = 4

  override def handle(scene: Scene, column: String): Option[MetricData] = {
    if (scene.sceneCode == MetricConstants.SCENE_MAIN || column == scene.label) return None
    val data = new CorrMetricData()
    if(DataFrameUtil.isNumericType(scene.dataFrame, column)){
      buildNumericTypeCorrMetric(scene, column, data)
    }else if(MetricUtil.preditIsDateType(scene.dataFrame, column)){
      buildDateTypeCorrMetric(scene, column, data)
    }else{
      None
    }
  }

  private def buildNumericTypeCorrMetric(scene: Scene, column: String, data: CorrMetricData): Option[CorrMetricData] = {
    //排除空值和缺失值
    val notNullAndNoMissingValueDf = scene.dataFrame
      .filter(col(column).isNotNull)
      .select(col(column).cast(DoubleType), col(scene.label).cast(DoubleType))
      .filter(!col(column).isin(metricConfig.getNumberMissingValue: _*))

    buildCorrMetricCommon(data, notNullAndNoMissingValueDf)
  }

  private def buildDateTypeCorrMetric(scene: Scene, column: String, data: CorrMetricData): Option[CorrMetricData] = {
    //排除空值和缺失值
    val notNullAndNoMissingValueDf = scene.dataFrame.filter(col(column).isNotNull)
      .select(datediff(col(column).substr(0, 10).cast(DateType), to_date(lit("1970-01-01"), "yyyy-MM-dd")).cast(DoubleType).as(column), col("label").cast(DoubleType))
      .filter(!col(column).isin(metricConfig.getNumberMissingValue:_*))

    buildCorrMetricCommon(data, notNullAndNoMissingValueDf)
  }

  private def buildCorrMetricCommon(data: CorrMetricData, notNullAndNoMissingValueDf: Dataset[Row]): Option[CorrMetricData] = {

    if(notNullAndNoMissingValueDf.count() <= 1) return None

    val x: RDD[Double] = notNullAndNoMissingValueDf.rdd.map(_.getDouble(0))
    val y: RDD[Double] = notNullAndNoMissingValueDf.rdd.map(_.getDouble(1))
    val corr = Statistics.corr(x, y)
    data.setCorr(corr)

    Some(data)
  }


}
