package com.github.skycrab.asset.job.service

import com.github.skycrab.asset.job.bean.PsiBinGroup
import org.apache.spark.sql.DataFrame

/**
  * Created by yihaibo on 2019-11-07.
  */
trait PsiService {
  /**
    * 计算psi
    * @param trainDataframe  训练集
    * @param testDataframe  测试集
    * @param column
    * @return
    */
  def calPsi(trainDataframe: DataFrame, testDataframe: DataFrame, column: String): PsiBinGroup

}
