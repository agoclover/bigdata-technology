package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//import scala.util.parsing.json.JSON

/**
  * Author: Felix
  * Date: 2020/7/11
  * Desc:  RDD处理Json数据
  *       本质就是对文本文件的处理
  */
object Spark05_Json {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\test.json")
    //Spark自带的json处理，已过时
    //val res: RDD[Option[Any]] = rdd.map(JSON.parseFull)

//    val res =  rdd.map(JSON.parse(_))

//    res.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
