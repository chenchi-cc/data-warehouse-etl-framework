package com.darrenchan.common.utils

import org.apache.commons.cli.{CommandLine, Options, PosixParser}

object ArgumentParser {
  val argOptions: Options = new Options()
    .addOption("e", "env", true, "job environment")
    .addOption("r", "grass_region", true, "grass region")
    .addOption("d", "grass_date", true, "grass date")
    .addOption("h","grass_hour",true,"grass hour")
    .addOption("l", "layer", true, "layer")
    .addOption("tb", "table", true, "table")
    .addOption("lp", "log_path", true, "log path for ingesting from kafka")
    .addOption("dp", "db_path", true, "log path for ingesting from db")
    .addOption("hp", "hdfs_path", true, "hdfs path of hive table")
    .addOption("rc", "region_column", true, "region columns of hive table, comma separated")


  val datePattern = "[0-9]{4}-[0-1][0-9]-[0-3][0-9]"

  def parseArguments(args: Array[String]): CommandLine = {
    val cmd = new PosixParser().parse(argOptions, args)

    for (option <- Seq("env", "grass_date")) {
      val value = cmd.getOptionValue(option)
      assert(value != null && value.nonEmpty, s"invalid value for argument $option")
    }

    assert(cmd.getOptionValue('d').matches(datePattern), "invalid grass-date format")

    cmd
  }
}
