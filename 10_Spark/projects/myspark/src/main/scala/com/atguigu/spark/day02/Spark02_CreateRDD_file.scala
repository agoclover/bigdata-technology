package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 通过读取外部文件创建RDD
  */
object Spark02_CreateRDD_file {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //读取本地外部文件创建RDD
    //val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\1.txt")
    //读取HDFS文件创建RDD
    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:9820/input")

    rdd.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }
}
