package com.darrenchan.dw

import com.darrenchan.common.BasicMain
import com.darrenchan.common.utils.DateHelper.getLocalTimeFromUtc
import com.darrenchan.common.utils.ResourceReader
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object CommonEntry extends BasicMain {
  def main(args: Array[String]): Integer = {
    prepare(args)

    var paras = scala.collection.mutable.Map(
      "grassDate" -> grassDate,
      "grassRegion" -> grassRegion,
      "grassRegionLower" -> grassRegion.toLowerCase,
      "schema" -> schema
    )
    if (StringUtils.isNotBlank(grassHour)) {
        paras += ("grassHour" -> grassHour)
        val (localDate, localHour) = getLocalTimeFromUtc(grassRegion, grassDate, grassHour.toInt)
//        val (localDate, localHour) = getTimeFromTimezone("SG", "ID", grassDate, grassHour.toInt)
        paras += ("localDate" -> localDate)
        paras += ("localHour" -> localHour.toString)
    }

    /*create dest table if needed*/
    val createTableSql = ResourceReader.read(s"sqls/ddl/${layer}/${tableName}.sql", paras)
    logger.info(createTableSql)
    spark.sql(createTableSql)

    /*get dml*/
    var partition_statement = s"grass_date='${grassDate}'"
    var partition_path = s"dt=${grassDate}"
    if (grassHour != null) {
      partition_statement = s"${partition_statement}, grass_hour='${grassHour}'"
      partition_path = s"${partition_path}/hour=${grassHour}"
    }
    val tmpView = s"tmp_view_${tableName}"

    if (logPath != null) {
      val path = s"hdfs://xxxx/${logPath}/${partition_path}/"
      if (!pathExists(path)) {
        logger.info("filepath {} does not exists.", path)
        spark.stop()
        return  -1

      }
      spark.read.load(path)/*.drop("_timestamp")*/.drop("dt").drop("hour").createOrReplaceTempView(tmpView)
      //execSql = get_exec_sql(layer, tableName, paras, partition_statement, tmpView)
    } else if (dbPath != null) {
      val path = s"hdfs://xxxx/${dbPath}/grass_date=${grassDate}"
      // 创建tmp view供execSql使用，drop掉分区列
      spark.read.load(path).drop("grass_schema").drop("grass_sharding").createOrReplaceTempView(tmpView)
      // 优先读取resource目录sql文件，适用于数据落ods时进行时区转换等
      //execSql = get_exec_sql(layer, tableName, paras, partition_statement, tmpView)
    } else if (hdfsPath != null) {
      val path = hdfsPath
      if (!pathExists(path)) {
        logger.info("hdfspath {} does not exists.", path)
        spark.stop()
        return -1
      }

      val readLoad: DataFrame = spark.read.load(path)
      if(regionColumn != null) {
        val columns: Array[String] = regionColumn.split(",")
        columns.map(column => readLoad.drop(column))
      }
      readLoad.cache().createOrReplaceTempView(tmpView)
    }

    val execSql = get_exec_sql(layer, tableName, paras, partition_statement, tmpView)

    logger.info(execSql)
    spark.sql(execSql)

    spark.stop()
    return 0
  }

  def get_exec_sql(dwLayer: String, tableName: String, paras: mutable.Map[String, String], partition_statement: String, view: String): String = {
    if (ResourceReader.exist(s"sqls/dml/${dwLayer}/${tableName}.sql")) {
      ResourceReader.read(s"sqls/dml/${dwLayer}/${tableName}.sql", paras)
    } else {
      s"""
         |insert overwrite table ${schema}.${tableName} partition(${partition_statement})
         |select /*+REPARTITION(1)*/ * from ${view}
         |""".stripMargin
    }
  }

  def pathExists(path: String): Boolean = {
    val filePath = new Path(path)
    val fileSystem = filePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val size = if (fileSystem.exists(filePath)) {
      fileSystem.getContentSummary(filePath).getLength
    } else {
      0
    }
    size > 0
  }
}
