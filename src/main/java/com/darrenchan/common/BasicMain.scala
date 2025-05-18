package com.darrenchan.common

import com.darrenchan.common.udf.CommonUdf
import com.darrenchan.common.utils.{ArgumentParser, Constant}
import org.apache.commons.cli.CommandLine
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

class BasicMain {
  val logger: Logger = LogManager.getLogger

  implicit var spark: SparkSession = _
  var cmd: CommandLine = _
  var env: String = _
  var grassRegion: String = _
  var grassDate: String = _
  var schema: String = _
  var grassHour: String = _
  /*var regionPath: String = _
  var regionTable: String = _
  var regionSchema: String= _*/
  var tableName: String= _
  var layer: String = _
  //ingest from kafka
  var logPath: String = _
  //ingest from db
  var dbPath: String = _
  var hdfsPath: String = _
  var regionColumn: String = _

  /*def prepare(args: Array[String]): Unit = {
    prepare(args, null, null)
  }*/

  def prepare(args: Array[String]/*, dwLayer: String, table: String*/) = {

    // 入参解析和校验
    cmd = ArgumentParser.parseArguments(args)

    env = cmd.getOptionValue("e")
    grassRegion = cmd.getOptionValue("r")
    grassDate = cmd.getOptionValue("d")
    grassHour = cmd.getOptionValue("h")
    layer = cmd.getOptionValue("l")
    tableName = cmd.getOptionValue("tb")
    logPath = cmd.getOptionValue("lp")
    dbPath = cmd.getOptionValue("dp")
    hdfsPath = cmd.getOptionValue("hp")
    regionColumn = cmd.getOptionValue("rc") //逗号分隔

    // 通过env获取schema
    schema = Constant.SCHEMA(env)

    logger.info(s"logg  env:${env},grassRegion:${grassRegion},grassDate:${grassDate}")

    spark = SparkSession
      .builder()
      .appName("friends_mart_%s_%s_%s_%s_%s".
        format(layer, tableName, grassRegion, grassDate, env))
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition","true")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.max.dynamic.partitions.pernode","2500")
      .config("spark.locality.wait", "1")
      .config("spark.debug.maxToStringFields","1000")
      .config("spark.sql.adaptive.enabled","true")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes","134217728")
      .config("hive.merge.mapfiles","true")
      .config("hive.merge.mapredfiles","true")
      .config("spark.sql.adaptive.minNumPostShufflePartitions","1")
      .config("spark.sql.adaptive.maxNumPostShufflePartitions","600")//数据量超过100g需要再调整
      .config("hive.merge.size.per.task","268435456")
      .config("spark.sql.hive.mergeFiles","true")
      .config("hive.merge.smallfiles.avgsize","268435456")
      .config("spark.sql.files.openCostInBytes","51943040")
      .config("spark.sql.hive.convertInsertingPartitionedTable","false")
      .config("spark.yarn.maxAppAttempts","6")
      .config("spark.yarn.max.executor.failures","1000")
      .config("spark.yarn.executor.memoryOverhead","2048")
      .config("spark.network.timeout","2000")
      .config("spark.shuffle.io.retryWait","10")
      .config("spark.shuffle.io.maxRetries","5")
      .config("spark.core.connection.ack.wait.timeout","300")
      .config("spark.sql.storeAssignmentPolicy","LEGACY")

      .config("spark.sql.autoBroadcastJoinThreshold","1073741824")
      .config("spark.sql.session.timeZone",Constant.TIMEZONE.getOrElse(grassRegion,"Asia/Singapore"))
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .getOrCreate()

    spark.udf.register("bitmap_contains", CommonUdf.bitmapContains _)
    spark.udf.register("split_head_abtest_info", CommonUdf.splitHeadAbtestInfo _)

    val sqlContext = spark.sqlContext
  }
}
