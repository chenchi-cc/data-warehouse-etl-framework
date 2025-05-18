package com.darrenchan.common.udf

import org.apache.logging.log4j.{LogManager, Logger}
import org.roaringbitmap.longlong.Roaring64NavigableMap

import java.io.{ByteArrayInputStream, DataInputStream}
import scala.util.Random

object CommonUdf {
  val logger: Logger = LogManager.getLogger
  def bitmapContains(binary: Array[Byte], userId: Long): Boolean = {
    val begin = System.currentTimeMillis()
    val res = if (null != binary) {
      var bitmap = new Roaring64NavigableMap()
      bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(binary.asInstanceOf[Array[Byte]])))
      bitmap.contains(userId)
    } else {
      false
    }
    val end = System.currentTimeMillis()
    val elapsed = end - begin
    val random = new Random()
    val percent = random.nextInt(100)
    if (percent < 5) {
      logger.info(s"input $userId res $res cost $elapsed")
    }
    res
  }

  def  splitHeadAbtestInfo(abtest:String):Array[String]={
    try {
      val abtestStr = abtest.substring(abtest.indexOf('@') + 1)
      val numStrList = abtestStr.split("[A-Z]+")
      val alphabetStrList = abtestStr.split("[0-9]+")
      val tmpinfo: StringBuilder = new StringBuilder
      for(i <- 0 to alphabetStrList.length-1){
        var abtest_info_split = alphabetStrList(i).split("")
        var countNum=1
        for(e <- abtest_info_split) {
          if(countNum%2==0){
            tmpinfo.append(e)
            tmpinfo.append(numStrList(i+1))
            tmpinfo.append(",")
          }else{
            tmpinfo.append(e)
          }
          countNum=countNum+1
        }
      }
      tmpinfo.toString().split(",")
    } catch {
      case exception: Exception => null
    }
  }
}
