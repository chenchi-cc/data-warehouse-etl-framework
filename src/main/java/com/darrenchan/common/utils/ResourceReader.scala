package com.darrenchan.common.utils

import scala.collection.mutable
import scala.io.Source

object ResourceReader {
  val PATH_PREFIX = "/com/darrenchan/"
  /**
   * @param path  相对于resources目录/com/darrenchan/的文件路径
   * @param paras 需要做变量替换的参数map，sql参数统一写为如'${grass_date}'的形式
   * @return 读取的文件内容string
   */
  def read(path: String, paras: mutable.Map[String, String]): String = {
    val stream = getClass.getResourceAsStream(s"${PATH_PREFIX}/${path}")
    var content = Source.fromInputStream(stream).getLines().mkString("\n")
    for ((k, v) <- paras) {
      content = content.replace("${" + k + "}", v)
    }
    content
  }

  def exist(path: String): Boolean = {
    val url = getClass.getResource(s"${PATH_PREFIX}/${path}")
    url != null
  }
}
