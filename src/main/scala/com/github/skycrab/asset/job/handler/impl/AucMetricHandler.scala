package com.github.skycrab.asset.job.handler.impl

import com.github.skycrab.asset.job.common.Logging
import com.github.skycrab.asset.job.util.DataFrameUtil
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.springframework.stereotype.Component

@Component
class AucMetricHandler extends Logging {

  def handle(joinDf: DataFrame, column: String, labelColumn: String): Unit = {
    logInfo(s"start calculate auc metric,column:${column}")
    val isNumericType = DataFrameUtil.isNumericType(joinDf, column)
    if (isNumericType) {
      val auc = calculate(joinDf, column, labelColumn)
      logInfo(s"column ${column} auc: ${auc}")
    } else {
      logInfo(s"column ${column} is not numeric,do not calculate auc metric")
    }
  }

  /**
   * 1.筛选字段
   * 2.获取训练集和测试集
   * 3.训练模型
   * 4.应用模型
   * 5.计算auc
   *
   * @param joinDf
   * @param column
   * @param labelColumn
   * @return``````
   */
  private def calculate(joinDf: DataFrame, column: String, labelColumn: String): Double = {
    val (trainDf, testDf) = getTrainAndTestDf(joinDf, column)
    val model = getModel(trainDf, labelColumn)
    val predictions = model.transform(testDf)
    calculateAuc(predictions, labelColumn)
  }

  /**
   * 按照70 30将数据范围训练集和测试集
   *
   * @param df
   * @param column
   * @return
   */
  private def getTrainAndTestDf(df: DataFrame, column: String): (DataFrame, DataFrame) = {
    val va = new VectorAssembler()
      .setInputCols(Array(column))
      .setOutputCol("features")
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val trainDf = va.transform(trainingData)
    val testDf = va.transform(testData)
    (trainDf, testDf)
  }

  /**
   * 获取决策树模型
   *
   * @return
   */
  private def getModel(trainDf: DataFrame, labelColumn: String): DecisionTreeClassificationModel = {
    val dt = new DecisionTreeClassifier()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
    val model = dt.fit(trainDf)
    val treeModel = model.asInstanceOf[DecisionTreeClassificationModel]
    println(s"Learned classification tree model:\n ${treeModel.toDebugString}")

    model
  }

  /**
   * 计算auc值
   * @param predictionsDataframe
   * @param labelColumn
   * @return
   */
  private def calculateAuc(predictionsDataframe: DataFrame, labelColumn: String): Double = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelColumn)
      .setMetricName("areaUnderROC")

    evaluator.evaluate(predictionsDataframe)
  }
}
