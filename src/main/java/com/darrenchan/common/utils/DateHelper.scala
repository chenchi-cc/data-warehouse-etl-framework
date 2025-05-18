package com.darrenchan.common.utils

import com.darrenchan.common.utils.Constant.TIMEZONE
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId}
import java.util.{Calendar, Date}

object DateHelper {
  def getGrassDateFromUtc(tzType: String, region: String, utcDate: String, hour: Int): String = {
    val grassZone = ZoneId.of(TIMEZONE(if (tzType == "local") region else "SG"))

    LocalDateTime.parse(f"$utcDate%sT$hour%02d", Constant.DATE_HOUR_FORMATTER)
      .atZone(ZoneId.of("UTC"))
      .withZoneSameInstant(grassZone)
      .format(Constant.DATE_FORMATTER)
  }



  val getGrassDateFromUtcUdf: UserDefinedFunction =
    udf((tzType: String, region: String, date: String, hour: Int) => {
      getGrassDateFromUtc(tzType, region, date, hour)
    })

  def getUtcTimeFromLocal(region: String, localDate: String, hour: Int): (String, Int) = {
    val utcTime = LocalDateTime
      .parse(f"$localDate%sT$hour%02d", Constant.DATE_HOUR_FORMATTER)
      .atZone(ZoneId.of(TIMEZONE(region)))
      .withZoneSameInstant(ZoneId.of("UTC"))

    (utcTime.toLocalDate.toString, utcTime.getHour)
  }

  def getLocalTimeFromUtc(region: String, localDate: String, hour: Int): (String, Int) = {
    val utcTime = LocalDateTime
      .parse(f"$localDate%sT$hour%02d", Constant.DATE_HOUR_FORMATTER)
      .atZone(ZoneId.of("UTC"))
      .withZoneSameInstant(ZoneId.of(TIMEZONE(region)))

    (utcTime.toLocalDate.toString, utcTime.getHour)
  }

  def getTimeFromTimezone( source: String,target: String, localDate: String, hour: Int): (String, Int) = {
    val targetTime = LocalDateTime
      .parse(f"$localDate%sT$hour%02d", Constant.DATE_HOUR_FORMATTER)
      .atZone(ZoneId.of(TIMEZONE(source)))
      .withZoneSameInstant(ZoneId.of(TIMEZONE(target)))

    (targetTime.toLocalDate.toString, targetTime.getHour)
  }


  /**
   * 设置grassRegion对应的spark 环境的timeZone
   */
  def setSparkEnvTimeZone(grassReion: String)(implicit spark: SparkSession): Unit ={
    spark.conf.set("spark.sql.session.timeZone",TIMEZONE.getOrElse(grassReion,"Asia/Singapore"))
  }
  def getDiffDate(grassDate:String,diffDays:Int=0): String ={

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date = new Date()
    date = dateFormat.parse(grassDate)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE,diffDays)
    dateFormat.format(cal.getTime())
  }

  def getAgoTimeHour(grassDate:String, grassHour: Int, hour: Int): (String, String) = {
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dayTime = dayFormat.parse(grassDate)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dayTime)
    cal.add(Calendar.HOUR, grassHour + hour)
    val hourFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val resHourDate = hourFormat.format(cal.getTime()).split(" ")
    (resHourDate(0), resHourDate(1))
  }
}
