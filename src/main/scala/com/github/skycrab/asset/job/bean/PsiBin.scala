package com.github.skycrab.asset.job.bean

/**
  * Created by yihaibo on 2019-11-07.
  */

/**
  *
  * @param binIndex bin索引, -1为null分箱
  * @param isUniqBin 是否是单独一箱
  */
case class PsiBin(binIndex: Int, isUniqueBin:Boolean, trainCount: Long, testCount: Long, trainTotal: Long, testTotal: Long) {
  /**
    * Calculate the actual PSI value from comparing the values.
    * Update the actual value to a very small number if equal to zero
    */
  val testPercent: Double = if(testCount == 0 || testTotal == 0) 0.0001  else testCount.toDouble/testTotal
  val trainPercent: Double = if(trainCount == 0 || trainTotal == 0) 0.0001  else trainCount.toDouble/trainTotal

  lazy val psi: Double = {
    (testPercent-trainPercent)*math.log(testPercent/trainPercent)
  }
}


/**
  *
  * @param bins 分bin统计
  * @param labels 等频分箱labels
  * @param uniqueLabels 单独组分箱labels
  */
case class PsiBinGroup(bins: Array[PsiBin], labels: Array[_], uniqueLabels: Array[_]) {

  /**
    * PSI < 0.1: no significant population change
    * PSI < 0.2: moderate population change
    * PSI >= 0.2: significant population change
    */
  lazy val psi: Double = bins.map(_.psi).sum

  override def toString: String = s"${bins.toList}, ${labels.toList}, ${uniqueLabels.toList}"
}
