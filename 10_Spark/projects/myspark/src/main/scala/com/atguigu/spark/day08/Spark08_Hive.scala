package com.atguigu.spark.day08

import org.apache.spark.sql.SparkSession

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: 连接外部Hive测试
  */
object Spark08_Hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show tables").show
  }
}
