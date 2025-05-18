package com.darrenchan.common.utils

import java.time.format.DateTimeFormatter

object Constant {
  val DATE_FORMAT = "yyyy-MM-dd"
  val DATE_HOUR_FORMAT = "yyyy-MM-dd'T'HH"
  val DATE_HOUR_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_HOUR_FORMAT)
  val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT)

  val TIMEZONE: Map[String, String] = Map(
    "SG" -> "Asia/Singapore", //新加坡
    "TW" -> "Asia/Taipei", //台湾
    "MY" -> "Asia/Kuala_Lumpur", //马来西亚
    "PH" -> "Asia/Manila", //菲律宾
    "ID" -> "Asia/Jakarta", //印尼
    "TH" -> "Asia/Bangkok", //泰国
    "VN" -> "Asia/Ho_Chi_Minh", //越南
    "BR" -> "Etc/GMT+3", //巴西
    "MX" -> "America/Mexico_City", //墨西哥
    "CL" -> "America/Santiago", //智利
    "CO" -> "America/Bogota"  //哥伦比亚
  )

  //库名
  val SCHEMA: Map[String, String] = Map(
    "live" -> "live_cc",
    "staging" -> "staging_cc",
    "test" -> "dev_cc"
  )
}
