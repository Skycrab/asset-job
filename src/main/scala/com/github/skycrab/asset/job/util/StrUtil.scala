package com.github.skycrab.asset.job.util

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by yihaibo on 2019-11-08.
  */
object StrUtil {

  private val LinePattern = "_(\\w)".r

  /**
    * 驼峰转化
    */
  def toHump(variable: String): String = {
    val matcher = LinePattern.pattern.matcher(variable.toLowerCase())
    val sb = new StringBuffer()
    while (matcher.find()) {
      matcher.appendReplacement(sb, matcher.group(1).toUpperCase())
    }
    matcher.appendTail(sb)
    sb.toString
  }

  /**
    * 拼接where
    * @return
    */
  def concatWhere(where: Map[String, _]): String = {
    val s = for((k, v) <- where) yield if(v.isInstanceOf[String]) s"${k}='${v}'" else s"${k}=${v}"
    s.mkString(" and ")
  }

  /**
    * 将json转为map
    * @param value
    * @return
    */
  def toKeyStringMap(value: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(value).extract[Map[String, Any]]
  }

  /**
    * sql where条件
    */
  def where(where: Map[String, _]): String = {
    if(where.isEmpty) {
      ""
    }else {
      s"where ${StrUtil.concatWhere(where)}"
    }
  }

  def toString(v: Any): String = {
    if(v != null) v.toString else ""
  }

  /**
    * 是否是日期类型
    * @param date 2020-02-02 || 2020-02-02 00:00:00
    * @return
    */
  def isDateOrDateTimeType(date: AnyRef): Boolean = {
    val dateStr = date.toString
    isDateType(dateStr) || isDateTimeType(dateStr)
  }

  /**
    * 是否是日期类型
    * @param date 20200202 || 20200202 00:00:00
    * @return
    */
  def isDateOrDateTimeTypeWithoutDash(date: AnyRef): Boolean = {
    val dateStr = date.toString
    isDateTypeWithoutDash(dateStr) || isDateTimeTypeWithoutDash(dateStr)
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val dateSet = Set("0000-00-00", "9999-99-99")
  def isDateType(dateStr: String): Boolean = {
    dateSet.contains(dateStr) ||
      (dateStr.length == 10 && Try(LocalDate.parse(dateStr, dateFormatter)).isSuccess)

  }

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val dateTimeSet = Set("0000-00-00 00:00:00", "9999-99-99 00:00:00")
  def isDateTimeType(dateStr: String): Boolean = {
    dateTimeSet.contains(dateStr) ||
      (dateStr.length == 19 && Try(LocalDateTime.parse(dateStr, dateTimeFormatter)).isSuccess)
  }


  private val dateWithoutDashFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val dateWithoutDashSet = Set("00000000", "99999999")
  def isDateTypeWithoutDash(dateStr: String): Boolean = {
    dateWithoutDashSet.contains(dateStr) ||
      (dateStr.length == 8 && Try(LocalDate.parse(dateStr, dateWithoutDashFormatter)).isSuccess)
  }

  private val dateTimeWithoutDashFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss")
  private val dateTimeWithoutDashSet = Set("00000000 00:00:00", "99999999 00:00:00")
  def isDateTimeTypeWithoutDash(dateStr: String): Boolean = {
    dateTimeWithoutDashSet.contains(dateStr) ||
      (dateStr.length == 17 && Try(LocalDateTime.parse(dateStr, dateTimeWithoutDashFormatter)).isSuccess)
  }

   def getPatitonToEpochday(patition: String): Long = {
    val patitionMap = HiveUtil.partitionToMap(patition)
    if (patitionMap.contains("dt")) {
      val dt = patitionMap("dt")
      val resPatition = if(StrUtil.isDateOrDateTimeTypeWithoutDash(dt)){
        LocalDate.parse(dt.substring(0, 8), DateTimeFormatter.ofPattern("yyyyMMdd")).toEpochDay
      }else{
        LocalDate.parse(dt.substring(0, 10), DateTimeFormatter.ofPattern("yyyy-MM-dd")).toEpochDay
      }
      resPatition
    } else{
      val resPatition = LocalDate.of(patitionMap("year").toInt, patitionMap("month").toInt, patitionMap("day").toInt).toEpochDay
      resPatition
    }
  }

  /**
    * 获取向前第n天日期
    * @param date 2020-02-02 || year=2020/month=02/day=02
    * @return [2020-02-02 , year=2020/month=02/day=02]
    */
  def getLastDateByDays(date: String , days: Int):LocalDate  = {
    val resDate = if(isDateType(date)){
      LocalDate.parse(date.substring(0, 10), DateTimeFormatter.ofPattern("yyyy-MM-dd")).minusDays(days)
    }else{
      val patitionMap = HiveUtil.partitionToMap(date)
      LocalDate.of(patitionMap("year").toInt, patitionMap("month").toInt, patitionMap("day").toInt).minusDays(days)
    }
    resDate
  }

  /**
    * 获取前n天日期
    *
    * @return
    */
  def getLastDatesByDays(date: String , days: Int): Array[LocalDate] = {
    val dateArray = new ArrayBuffer[LocalDate]
    for(i <- 0 to days) {
      dateArray += getLastDateByDays(date: String , i: Int)
    }
    dateArray.toArray
  }

  /**
    * 2位整数，不足向前补0
    *
    * @return
    */
  def zeroPadding2(input: Int): String = {
    f"$input%02d"
  }

}
