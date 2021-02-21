package com.github.skycrab.asset.job.util

import org.junit.Assert._
import org.junit.Test

/**
  * Created by yihaibo on 2019-11-08.
  */
class StrUtilTest {

  @Test
  def toHumpTest(): Unit = {
    assertEquals(StrUtil.toHump("a_b_c"), "aBC")
    assertEquals(StrUtil.toHump("a1_b2_c3"), "a1B2C3")
  }

  @Test
  def concatWhereTest(): Unit = {
    val where: Map[String, _] = Map("year" -> "2019")
    assertEquals(StrUtil.concatWhere(where), "year='2019'")

    val where2: Map[String, _] = Map("year" -> "2019", "month" -> "10", "day" -> "10")
    assertEquals(StrUtil.concatWhere(where2), "year='2019' and month='10' and day='10'")

    val where3: Map[String, _] = Map("year" -> 2019, "month" -> 10, "day" -> 10)
    assertEquals(StrUtil.concatWhere(where3), "year=2019 and month=10 and day=10")

  }

  @Test
  def dateTest(): Unit = {
    assertEquals(StrUtil.isDateOrDateTimeType("2020-02-02"), true)
    assertEquals(StrUtil.isDateOrDateTimeType("2020-02-02 00:00:00"), true)
    assertEquals(StrUtil.isDateOrDateTimeType("1970-01-01"), true)
    assertEquals(StrUtil.isDateOrDateTimeType("0000-00-00"), true)
    assertEquals(StrUtil.isDateOrDateTimeType("2020-02102"), false)
    assertEquals(StrUtil.isDateOrDateTimeType("hello"), false)
  }

}
