package com.github.skycrab.asset.job.bean

import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}

/**
  * Created by yihaibo on 2019-11-07.
  */
class InputParameterTest {

  val _thrown = ExpectedException.none

  @Rule
  def thrown = _thrown

  @Test
  def correctTest(): Unit = {
    val input = InputParameter("db.table", "year=2019/month=09/day=28", "hive", "[]", "[]")

    assertEquals(input.tableName, "db.table")
    assertEquals(input.db, "db")
    assertEquals(input.table, "table")
    assertEquals(input.partitionMap().getOrElse("year", ""), "2019")
    assertEquals(input.partitionMap().getOrElse("day", ""), "28")
    assert(input.subMetricTypes.size > 1)
  }


}
