package com.github.skycrab.asset.job.util

import java.{util => ju}

/**
 * @Author yihaibo
 * @Create 2021/8/26
 */
object CommonUtil {
  def toDouble(feature: Any): Double = {
    feature match {
      case d: Double => d
      case f: Float => f.toDouble
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case b: BigDecimal => b.toDouble
      case _ => feature.toString.toDouble
    }
  }

  def toInt(feature: Any): Int = {
    feature match {
      case i: Int => i
      case l: Long => l.toInt
      case d: Double => d.toInt
      case f: Float => f.toInt
      case b: BigDecimal => b.toInt
      case _ => feature.toString.toInt
    }
  }

  /**
   * binarySearch(Array(Double.NegativeInfinity,-497.5, 7.5, Double.PositiveInfinity), 4)
   */
  def binarySearch(splits: Array[Double], feature: Double): Int = {
    if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new RuntimeException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

}
