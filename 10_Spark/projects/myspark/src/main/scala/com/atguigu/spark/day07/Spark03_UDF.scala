package com.atguigu.spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author: Felix
  * Date: 2020/7/14
  * Desc: 自定义UDF函数
  */
object Spark03_UDF {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQLDemo")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //注册一个函数
    spark.udf.register("sayHi",(name:String)=>{"hello-->" + name})

    //读取json文件创建df
    val df: DataFrame = spark.read.json("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\test.json")

    //创建临时视图
    df.createOrReplaceTempView("people")

    //编写sql，查询临时视图
    spark.sql("select sayHi(username) from people").show
  }
}
