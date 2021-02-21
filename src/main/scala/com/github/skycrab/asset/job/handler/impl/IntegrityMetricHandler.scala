package com.github.skycrab.asset.job.handler.impl

import java.time.LocalDate
import java.util.{List => JList}

import com.github.skycrab.asset.client.enums.SubMetricType
import com.github.skycrab.asset.client.metric.MetricData
import com.github.skycrab.asset.client.metric.quality.IntegrityMetricData
import com.github.skycrab.asset.client.metric.quality.IntegrityMetricData.Group
import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.handler.ColumnMetricHandler
import com.github.skycrab.asset.job.util.NumberUtil
import com.github.skycrab.asset.job.bean.Scene
import com.github.skycrab.asset.job.config.MetricConfig
import com.github.skycrab.asset.job.util.{DataFrameUtil, MetricUtil, StrUtil}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.types.{DateType, DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import scala.collection.JavaConverters._

/**
  * Created by yihaibo on 2020-07-10.
  * 完整性指标处理器
  */
@Component
class IntegrityMetricHandler extends ColumnMetricHandler with Logging {

  @Autowired
  private var spark: SparkSession = _

  @Autowired
  private var metricConfig: MetricConfig = _

  /**
    * 子维度类型
    */
  override def subMetricType: SubMetricType = SubMetricType.INTEGRITY

  override def order = 1

  override def handle(scene: Scene, column: String): Option[MetricData] = {
    val df = scene.dataFrame
    df.createOrReplaceTempView("tmp")
    val data = new IntegrityMetricData()

    calStat(df, column, data)

    if(data.getIsNumeric) {
      data.setIsDateType(false)
      calMissingValueDetail(df, column, data)
      calNumberColFrequencyDistributions(df, column, data)
    }else {
      val isDataType = MetricUtil.preditIsDateType(df, column)
      data.setIsDateType(isDataType)
      if(isDataType) {
        calIllegalDateDetail(df, column, data)
        calDateColFrequencyDistributions(df, column, data)
      }
    }

    //离散变量top分组
    if(data.getDistinctCount < 1000) {
      data.setIsDiscreteVariable(true)
      calDiscreteVariableTops(df, column, data)
    }else {
      data.setIsDiscreteVariable(false)
    }

    Some(data)
  }

  /**
    * 计算基本统计信息
    */
  private def calStat(df: DataFrame, column: String, data: IntegrityMetricData): Unit = {
    val isNumericType = DataFrameUtil.isNumericType(df, column)

    val sql = if(isNumericType) {
      s"""
         |select
         |  count(*) totalCount,
         |  count(distinct $column) distinctCount,
         |  count($column) count,
         |  min($column) min,
         |  max($column) max,
         |  avg(cast($column as double)) avg,
         |  stddev(cast($column as double)) stddev,
         |  avg(case $column when 0 then null else cast($column as double) end) notZeroAvg,
         |  min(case $column when 0 then null else cast($column as double) end) notZeroMin
         |from tmp
      """.stripMargin
    }else {
      s"""
         |select
         |  count(*) totalCount,
         |  count(distinct $column) distinctCount,
         |  count($column) count,
         |  min($column) min,
         |  max($column) max
         |from tmp
      """.stripMargin
    }

    //logInfo(s"sql: $sql")
    val df2 = spark.sql(sql)
    val st = df2.collect()(0)

    val totalCount = st.getAs[Long]("totalCount")
    val notNullCount = st.getAs[Long]("count")
    val nullCount = totalCount-notNullCount

    data.setTotalCount(totalCount)
    data.setNullCount(nullCount)
    data.setNullProportion(NumberUtil.proportion(nullCount, totalCount))
    data.setDistinctCount(st.getAs[Long]("distinctCount"))
    data.setMax(StrUtil.toString(st.getAs("max")))
    data.setMin(StrUtil.toString(st.getAs("min")))

    data.setIsNumeric(isNumericType)

    if(isNumericType) {
      data.setAvg(NumberUtil.toPrecision(st.getAs[Double]("avg")))
      data.setStddev(NumberUtil.toPrecision(st.getAs[Double]("stddev")))
    }

  }

  /**
    * 计算缺失值详情
    */
  private def calMissingValueDetail(df: DataFrame, column: String, data: IntegrityMetricData): Unit = {
    val missingValueDf = df.select(col(column).cast(DoubleType))
      .filter(col(column).isin(metricConfig.getNumberMissingValue:_*))

    val missingValueDetail = missingValueDf.groupBy(column).count().collect().map(r => {
      val missingValue = r.getDouble(0).toString
      val count = r.getLong(1)
      val proportion = NumberUtil.proportion(count, data.getTotalCount)
      Group.builder().label(missingValue).count(count).total(data.getTotalCount).proportion(proportion).build()
    }).toList.asJava

    data.setMissingValueDetail(missingValueDetail)
  }

  private def calFrequencyDistributionsCommon(column: String, notNullAndNoMissingValueDf: DataFrame): (Array[(Int, Long)], Array[Double]) = {
    val sampleDf = DataFrameUtil.sample(notNullAndNoMissingValueDf, metricConfig.getSampleCount)

    val outPutCol = column + "_quantile"
    val discretizer = new QuantileDiscretizer()
      .setInputCol(column)
      .setOutputCol(outPutCol)
      .setNumBuckets(metricConfig.getDiscretizeNumBucket)

    val bucketizer = discretizer.fit(sampleDf)
    val splitDf = bucketizer.transform(notNullAndNoMissingValueDf)
    val countDf = splitDf.groupBy(outPutCol).count()

    val labels = bucketizer.getSplits.map(NumberUtil.toPrecision(_))
    //logInfo(s"labels: ${labels.toList}")
    //countDf.show(false)

    //index从小到大排序
    val labelCount = countDf.collect().map(r => {
      val index = r.getDouble(0).toInt
      val count = r.getLong(1)
      (index, count)
    }).sortBy(_._1)
    (labelCount, labels)
  }

  /**
    * 计算数字型分布信息
    */
  private def calNumberColFrequencyDistributions(df: DataFrame, column: String, data: IntegrityMetricData): Unit = {
    //分位数排除null和缺失值，且转为double类型
    val notNullAndNoMissingValueDf = df.filter(col(column).isNotNull)
      .select(col(column).cast(DoubleType))
      .filter(!col(column).isin(metricConfig.getNumberMissingValue:_*))

    if (notNullAndNoMissingValueDf.count() == 0) {
      return
    }

    val (labelCount, labels) :(Array[(Int, Long)], Array[Double]) = calFrequencyDistributionsCommon(column, notNullAndNoMissingValueDf)
    val numberLabels = labels.map(item => item.toString)

    val frequencyDistributions: JList[Group] = buildFrequencyDistributions(labelCount, numberLabels, data)
    data.setFrequencyDistributions(frequencyDistributions)
  }

  /**
    * 计算时间类型分布信息
    */
  private def calDateColFrequencyDistributions(df: DataFrame, column: String, data: IntegrityMetricData): Unit = {
    //分位数排除null和缺失值，且转为double类型
    //计算epochday
    val notNullAndNoMissingValueDf = df.filter(col(column).isNotNull)
      .select(datediff(col(column).substr(0, 10).cast(DateType), to_date(lit("1970-01-01"), "yyyy-MM-dd")).cast(DoubleType).as(column))
      .filter(!col(column).isin(metricConfig.getNumberMissingValue:_*))

    if (notNullAndNoMissingValueDf.count() == 0) {
      return
    }

    val (labelCount, labels):(Array[(Int, Long)], Array[Double]) = calFrequencyDistributionsCommon(column, notNullAndNoMissingValueDf)

    //正负无穷时间转化为无效值，由于不直接显示，可以接受
    val dateLabels: Array[String] = labels.map(item => LocalDate.of(1970, 1, 1).plusDays(item.toInt).toString)

    val frequencyDistributions: JList[Group] = buildFrequencyDistributions(labelCount, dateLabels, data)
    data.setFrequencyDistributions(frequencyDistributions)
  }

  private def buildFrequencyDistributions(labelCount: Array[(Int, Long)], labels: Array[String], data: IntegrityMetricData): JList[Group] = {
    val frequencyDistributions =
      labelCount.map {
        case (index, count) =>
          val proportion = NumberUtil.proportion(count, data.getTotalCount)
          Group.builder().label(MetricUtil.getQuantileLabel(index, labels)).count(count).total(data.getTotalCount).proportion(proportion).build()
      }.toList.asJava
    frequencyDistributions
  }

  /**
    * 统计日期不符合2xx的数据
    * .filter(!col(column).like("2%"))
    * .groupBy(col(column)).agg(count(column).as("cnt")).where(col("cnt")>1)
    */
  private def calIllegalDateDetail(df: DataFrame, column: String, data: IntegrityMetricData): Unit = {
    val illegalDateDf = df.select(col(column).cast(StringType))
      .filter(!col(column).like("2%"))
      .groupBy(col(column)).count()

    val illegalDateDetail = illegalDateDf.take(10).map(r => {
      val dateStr = r.getAs[String](0)
      val count = r.getAs[Long](1)
      val proportion = NumberUtil.proportion(count, data.getTotalCount)
      Group.builder().label(dateStr).count(count).total(data.getTotalCount).proportion(proportion).build()
    }).toList.asJava

    data.setIllegalDateDetail(illegalDateDetail)
  }

  /**
    * 离散变量计算分组top
    */
  private def calDiscreteVariableTops(df: DataFrame, column: String, data: IntegrityMetricData): Unit = {
    val notNullDf = df.filter(col(column).isNotNull)
    val topDf = notNullDf.groupBy(col(column))
      .agg(count(column).as("cnt")).where(col("cnt")>10)
      .orderBy(col("cnt").desc).limit(20)

    val topGroups = topDf.collect().map(r => {
      val label = r.get(0).toString
      val count = r.getLong(1)
      val proportion = NumberUtil.proportion(count, data.getTotalCount)
      Group.builder().label(label).count(count).total(data.getTotalCount).proportion(proportion).build()
    }).toList.asJava
    data.setDiscreteVariableTops(topGroups)
  }

}
