package com.atguigu.spark.day08

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: 将df数据写入到MySQL数据库
  */
object Spark07_MySQL_write {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._
    //创建df
    val df: DataFrame = spark.read.json("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\test.json")

    //df.write.format("jdbc")
    //方式1：通用的方式  format指定写出类型
    val ds: Dataset[People] = df.as[People]
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop202:3306/test")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    /*//方式2：通过jdbc方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop202:3306/test", "user", props)
*/

  }
}

case class People(name:String,age:Long)