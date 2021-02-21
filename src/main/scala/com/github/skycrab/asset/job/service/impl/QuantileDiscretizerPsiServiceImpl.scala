package com.github.skycrab.asset.job.service.impl

import java.time.LocalDate

import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.bean.{PsiBin, PsiBinGroup}
import com.github.skycrab.asset.job.config.MetricConfig
import com.github.skycrab.asset.job.service.PsiService
import com.github.skycrab.asset.job.util.{DataFrameUtil, MetricUtil}
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yihaibo on 2019-11-07.
  * 分位数psi
  */
@Service
class QuantileDiscretizerPsiServiceImpl extends PsiService with Logging {
  @Autowired
  private var spark: SparkSession = _

  @Autowired
  private var metricConfig: MetricConfig = _

  /**
    * 计算psi
    * @param trainDataframe  训练集
    * @param testDataframe  测试集
    * @param column
    * @return
    */
  override def calPsi(trainDataframe: DataFrame, testDataframe: DataFrame, column: String): PsiBinGroup = {
    //一期支持数字类型
    //新增对日期字符串类型对支持
    require(DataFrameUtil.isNumericType(testDataframe, column) || MetricUtil.preditIsDateType(testDataframe, column), s"${column} must be numeric type or date type")
    require(DataFrameUtil.isNumericType(trainDataframe, column) || MetricUtil.preditIsDateType(testDataframe, column), s"${column} must be numeric type or date type")

    if(DataFrameUtil.isNumericType(testDataframe, column)){
      val trainNumericTypeDfNotNull = getTrainNumericTypeDfNotNull(trainDataframe, column)
      val testNumericTypeDfNotNull = getTrainNumericTypeDfNotNull(testDataframe, column)
      buildNumericTpyePsi(trainDataframe, testDataframe, column, trainNumericTypeDfNotNull, testNumericTypeDfNotNull)
    }else{
      val trainDateTypeDfNotNull = getTrainDateTypeDfNotNull(trainDataframe, column)
      val testDateTypeDfNotNull = getTrainDateTypeDfNotNull(testDataframe, column)
      buildDateTpyePsi(trainDataframe, testDataframe, column, trainDateTypeDfNotNull, testDateTypeDfNotNull)
    }
  }

  private def getTrainDateTypeDfNotNull(trainDataframe: DataFrame, column: String): DataFrame = {
    trainDataframe.select(datediff(col(column).substr(0, 10).cast(DateType), to_date(lit("1970-01-01"), "yyyy-MM-dd")).cast(DoubleType).as(column)).filter(col(column).isNotNull)
  }

  private def getTrainNumericTypeDfNotNull(trainDataframe: DataFrame, column: String): DataFrame = {
    trainDataframe.filter(col(column).isNotNull).select(col(column).cast(DoubleType))
  }

  private def buildNumericTpyePsi(trainDataframe: DataFrame, testDataframe: DataFrame, column: String, trainDfNotNull: DataFrame, testDfNotNull: DataFrame): PsiBinGroup = {
    val trainTotal = trainDataframe.count()
    val testTotal = testDataframe.count()

    //step1 计算空值分箱
    val nullBins = calNullBins(trainDataframe, testDataframe, column, trainTotal, testTotal)

    //step2 计算占比大分箱
    val (uniqueBins, uniqueLables) = calUniqueBins(trainDfNotNull, testDfNotNull, column, trainTotal, testTotal)

    //step3 计算等频分箱
    val (quantileBins, quantileLabels) = calQuantileBins(trainDfNotNull, testDfNotNull, column, trainTotal, testTotal, uniqueLables)

    val mergeBins = ArrayBuffer[PsiBin]()
    Array(nullBins, uniqueBins, quantileBins).foreach(mergeBins.appendAll(_))

    PsiBinGroup(mergeBins.toArray, quantileLabels, uniqueLables)
  }

  private def buildDateTpyePsi(trainDataframe: DataFrame, testDataframe: DataFrame, column: String, trainDfNotNull: DataFrame, testDfNotNull: DataFrame): PsiBinGroup = {
    val trainTotal = trainDataframe.count()
    val testTotal = testDataframe.count()

    //step1 计算空值分箱
    val nullBins = calNullBins(trainDataframe, testDataframe, column, trainTotal, testTotal)

    //step2 计算占比大分箱
    val (uniqueBins, uniqueLables) = calUniqueBins(trainDfNotNull, testDfNotNull, column, trainTotal, testTotal)
    val dateUniqueLables: Array[String] = epochdayToDateLables(uniqueLables)

    //step3 计算等频分箱
    val (quantileBins, quantileLabels) = calQuantileBins(trainDfNotNull, testDfNotNull, column, trainTotal, testTotal, uniqueLables)
    val dateQuantileLabels: Array[String] = epochdayToDateLables(quantileLabels)

    val mergeBins = ArrayBuffer[PsiBin]()
    Array(nullBins, uniqueBins, quantileBins).foreach(mergeBins.appendAll(_))

    PsiBinGroup(mergeBins.toArray, dateQuantileLabels, dateUniqueLables)
  }

  private def epochdayToDateLables(uniqueLables: Array[Double]): Array[String] = {
    uniqueLables.map(item => LocalDate.of(1970, 1, 1).plusDays(item.toInt).toString)
  }

  /**
    * 计算null分箱
    *
    * @return
    */
  private def calNullBins(trainDataframe: DataFrame, testDataframe: DataFrame, column: String, trainTotal: Long, testTotal: Long): Array[PsiBin] = {
    val trainNullCount = trainDataframe.filter(col(column).isNull).count()
    val testNullCount = testDataframe.filter(col(column).isNull).count()
    if(trainNullCount > 0) {
      val isUniqueBin = true
      Array(PsiBin(-1, isUniqueBin, trainNullCount, testNullCount, trainTotal, testTotal))
    }else {
      Array.empty
    }
  }

  /**
    * 计算单独箱
    *
    * 找到占比最大且>20%，作为单独一箱，不然默认值占比较大将导致特征波动很小
    */
  private def calUniqueBins(trainDfNotNull: DataFrame, testDfNotNull: DataFrame, column: String, trainTotal: Long, testTotal: Long): (Array[PsiBin], Array[Double]) = {
    val df = trainDfNotNull.groupBy(column).count().filter(col("count") > trainDfNotNull.count()*0.2)
    val uniqueCounts = df.collect().map(s => (s.getDouble(0), s.getLong(1))).toMap
    val uniqueLables: Array[Double] = uniqueCounts.keys.toArray.sorted
    //    logInfo(s"column: $column, uniqueCounts: uniqueCounts, sort uniqueLables: ${uniqueLables.toList}")

    val isUniqueBin = true
    if(uniqueCounts.nonEmpty) {
      val testCounts = testDfNotNull.filter(col(column).isin(uniqueLables:_*)).groupBy(column).count().collect().map(s => (s.getDouble(0), s.getLong(1))).toMap
      val uniqueBins = uniqueLables.indices.map(i => {
        val label = uniqueLables(i)
        PsiBin(i, isUniqueBin, uniqueCounts(label), testCounts.getOrElse(label, 0L), trainTotal, testTotal)
      }).toArray

      (uniqueBins, uniqueLables)
    }else {
      (Array.empty, Array.empty)
    }
  }

  /**
    * 分位数分箱
    */
  private def calQuantileBins(trainDfNotNull: DataFrame, testDfNotNull: DataFrame, column: String, trainTotal: Long, testTotal: Long, uniqueLables: Array[Double]): (Array[PsiBin], Array[Double]) = {
    val outPutColumn = s"${column}_output"

    val (testDf, trainDf) = if(uniqueLables.isEmpty) {
      (testDfNotNull, trainDfNotNull)
    }else {
      (testDfNotNull.filter(!col(column).isin(uniqueLables:_*)), trainDfNotNull.filter(!col(column).isin(uniqueLables:_*)))
    }

    // 根据分位数初始化分箱
    val discretizer = new QuantileDiscretizer()
      .setInputCol(column)
      .setOutputCol(outPutColumn)
      .setNumBuckets(10)

    //trainDf可能为空
    if(trainDf.count() > 0) {
      val sampleDf = DataFrameUtil.sample(trainDf, metricConfig.getSampleCount)
      val bucketizer = discretizer.fit(sampleDf)
      val trainLabelCount = binCount(bucketizer, trainDf, outPutColumn)
      val testLabelCount = binCount(bucketizer, testDf, outPutColumn)
      val isUniqueBin = false

      val psiBins = for((label, count) <- trainLabelCount) yield PsiBin(label, isUniqueBin, count, testLabelCount.getOrElse(label, 0L), trainTotal, testTotal)
      val psiBinsSort = psiBins.toArray.sortBy(_.binIndex)
      (psiBinsSort, bucketizer.getSplits)
    }else {
      (Array.empty, Array.empty)
    }
  }

  /**
    * 计算分bin各区间数量
    * @param bucketizer
    * @param df
    * @param column
    * @return (各label数量,总人数)
    */
  private def binCount(bucketizer: Bucketizer, df: DataFrame, column: String): Map[Int, Long] = {
    val bins = bucketizer.transform(df)
    val binsCount = bins.groupBy(column).count().collect().map(s => (s.getDouble(0).toInt, s.getLong(1))).toMap
    binsCount
  }
}
