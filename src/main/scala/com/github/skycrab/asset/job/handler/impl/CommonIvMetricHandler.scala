package com.github.skycrab.asset.job.handler.impl

import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.util.CommonUtil.{binarySearch, toDouble, toInt}
import com.github.skycrab.asset.job.util.DataFrameUtil
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{QuantileDiscretizer, VectorAssembler}
import org.apache.spark.ml.tree.{CategoricalSplit, ContinuousSplit, InternalNode, Node}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, countDistinct, udf, when}
import org.springframework.stereotype.Component

import scala.collection.mutable.ArrayBuffer

@Component
class CommonIvMetricHandler extends Logging {

  /**
   * 最大分箱数
   */
  val MAX_BIN: Int = 5

  val BAD_CNT_FIELD = "badCnt"
  val GOOD_CNT_FIELD = "goodCnt"

  def handle(joinDf: DataFrame, column: String, labelColumn: String): Unit = {
    logInfo(s" start calculate iv metric,column:${column}")
    val isNumericType = DataFrameUtil.isNumericType(joinDf, column)
    if (!isNumericType){
      logInfo(s"column ${column} is not numeric,do not calculate iv metric")
      return
    }
    /*val df = joinDf.na.fill(NAN_VALUE, Array(column))*/

    //df.show(2, false)

    val (bucketDf, splitIndexColumn) = if(columnIsContinuous(joinDf, column)) {
      continuousBinSplit(joinDf, column, labelColumn)
    }else {
      categoricalBinSplit(joinDf,column, labelColumn)
    }
    bucketDf.show(10, false)

    val iv = calculateIv(bucketDf, column, splitIndexColumn, labelColumn)
    logInfo(s"column:${column} iv:${iv}")
  }

  /**
   * 连续变量分箱
   * @return (dataframe, 分箱索引列)
   */
  private def continuousBinSplit(df: DataFrame, column: String, labelColumn: String): (DataFrame, String) = {
    logInfo(s"column: ${column} is continuous")
    // 连续型特征的分箱，优先使用决策树分箱，没有合适分裂点，使用等频分箱
    var splits = treeSplit(df, column, labelColumn)
    if(splits.length == 2) {
      splits = quantileSplit(df, column, labelColumn)
    }

    val indexUdf = udf {feature: Any => binarySearch(splits, toDouble(feature))}
    // 新增列 -> splitIndex
    val splitIndexColumn = "splitIndex"
    val bucketDf = df.select(col("*"), indexUdf(col(column)).as(splitIndexColumn))

    (bucketDf, splitIndexColumn)
  }

  /**
   * 处理分类变量分箱
   * @return (dataframe, 分箱索引列)
   */
  private def categoricalBinSplit(df: DataFrame, column: String, labelColumn: String): (DataFrame, String) = {
    logInfo(s"column: ${column} is categorical")
    // 分类型特征分箱,直接根据类别进行groupby
    val splitIndexColumn = column
    (df, splitIndexColumn)
  }

  /**
   * 计算iv值
   * @param bucketDf  统计数据
   * @param column  计算列
   * @param splitIndexColumn 分箱列索引列名
   * @param labelColumn  样本列
   * @return
   */
  private def calculateIv(bucketDf: DataFrame, column: String, splitIndexColumn: String, labelColumn: String): Double = {
    // 按分箱列统计各分箱好坏数量
    val splitStatDf = bucketDf.groupBy(splitIndexColumn)
      .agg(count(when(col(labelColumn) === 1, true)).as(BAD_CNT_FIELD),
        count(when(col(labelColumn) === 0, true)).as(GOOD_CNT_FIELD))
    splitStatDf.show(false)

    val bins = splitStatDf.collect()
      .map(s => Bin(toInt(s.getAs[Any](splitIndexColumn)), s.getAs[Long](BAD_CNT_FIELD), s.getAs[Long](GOOD_CNT_FIELD)))
      .sortBy(_.splitIndex)

    val woebins = binToWoe(bins)
    val iv = woebins.map(_.iv).sum
    logInfo(s"column: ${column}, iv: ${iv},  woebins: ${woebins.toList}")
    iv
  }

  /**
   * woe转化
   */
  private def binToWoe(bins: Array[Bin]): Array[WoeBin] = {
    val goodTotal = bins.map(_.goodCnt).sum
    val badTotal = bins.map(_.badCnt).sum
    bins.map(bin => {
      // 防止计算WOE的时候分子或者分母为0, 造成WOE无穷大
      val goodCntCorrect = math.max(1, bin.goodCnt)
      val badCntCorrect = math.max(1, bin.badCnt)
      val gr = goodCntCorrect.toDouble / goodTotal
      val br = badCntCorrect.toDouble / badTotal
      val woe = math.log(gr/br)
      val iv = (gr-br) * woe
      WoeBin(bin.splitIndex, badCntCorrect, goodCntCorrect, badTotal, goodTotal, woe, iv)
    })
  }
  /**
   * 判断是否是离散型变量
   */
  private def columnIsContinuous(labelDf: DataFrame, column: String): Boolean = {
    val count: Long = labelDf.agg(countDistinct(col(column))).collect().head.getLong(0)
    logInfo(s"${column} count distinct ${count}")
    count > MAX_BIN
  }

  /**
   * 等频分箱
   * @return 分箱点 [10,20,30]
   */
  private def quantileSplit(labelDf: DataFrame, column: String, labelColumn: String): Array[Double] = {
    val outPutColumn = s"${column}_output"
    val discretizer = new QuantileDiscretizer()
      .setInputCol(column)
      .setOutputCol(outPutColumn)
      .setNumBuckets(6)

    val bucketizer = discretizer.fit(labelDf)
    val splits = bucketizer.getSplits
    logInfo(s"column: ${column} quantileSplit:${splits.toList}")
    splits
  }

  /**
   * 决策树分箱
   * @return 分箱点 [10,20,30]
   */
  private def treeSplit(labelDf: DataFrame, column: String, labelColumn: String): Array[Double] = {
    val va = new VectorAssembler()
      .setInputCols(Array(column))
      .setOutputCol("features")

    val df = va.transform(labelDf)

    val dt = new DecisionTreeClassifier()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")

    val model = dt.fit(df)
    val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
    logInfo(s"Learned classification tree model:\n ${treeModel.toDebugString}")

    val splits = ArrayBuffer[Double]()
    iterInternalNode(column, treeModel.rootNode, splits)
    val distinctSplits = getDistinctSplits(splits)
    logInfo(s"column: ${column} tree split:${distinctSplits}")
    distinctSplits.toArray
  }

  private def getDistinctSplits(splits: ArrayBuffer[Double]): ArrayBuffer[Double] = {
    splits.append(Double.NegativeInfinity)
    splits.append(Double.PositiveInfinity)
    splits.distinct.sorted
  }

  /**
   * 遍历决策树叶子节点，获取分箱阈值
   */
  private def iterInternalNode(column: String, node: Node, split: ArrayBuffer[Double]): Unit = {
    node match {
      case n: InternalNode =>
        iterInternalNode(column, n.leftChild, split)
        n.split match {
          case contSplit: ContinuousSplit =>
            split.append(contSplit.threshold)
          case catSplit: CategoricalSplit =>
            val categoriesStr = catSplit.leftCategories.mkString("{", ",", "}")
            logError(s"${column} appear unexpect CategoricalSplit, ${categoriesStr}")
        }
        iterInternalNode(column, n.rightChild, split)
      case _ =>
    }
  }
}

/**
 * 初始一组分bin统计结果
 * @param splitIndex  当前组序号
 * @param badCnt  当前组坏用户数量
 * @param goodCnt  当前组好用户数量
 */
case class Bin(splitIndex: Int, badCnt: Long, goodCnt: Long)


case class WoeBin(splitIndex: Int, badCnt: Long, goodCnt: Long, badTotal: Long, goodTotal: Long, woe: Double, iv: Double) {

  /**
   * margin_good_rate 边际好人率
   */
  val marginGoodRate: Double = goodCnt.toDouble / goodTotal

  /**
   * 边际坏人率
   */
  val marginBadRate: Double = badCnt.toDouble / badTotal

  /**
   * 坏客户占比
   */
  val badRate: Double = badCnt.toDouble / (badCnt+goodCnt)

  val binSize: Long = badCnt + goodCnt

  val total: Long = badTotal + goodTotal
}
