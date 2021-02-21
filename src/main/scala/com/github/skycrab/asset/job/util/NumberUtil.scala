package com.github.skycrab.asset.job.util

/**
  * Created by yihaibo on 2020-07-14.
  */
object NumberUtil {

  /**
    * 保留小数位
    */
  def toPrecision(v: Double, precision: Int =4): Double = {
    v.formatted(s"%.${precision}f").toDouble
  }

  /**
    * 计算 a/b 占比
    */
  def proportion(a: Long, b: Long, precision: Int =4): Double = {
    toPrecision(a * 1.0 / b, precision)
  }

  def toBoolean(value: Long): Boolean = {
    if(value > 0){
      true
    }else {
      false
    }
  }


}
