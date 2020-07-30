package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 默认的分区数
  *   -通过集合创建RDD
  *     默认分区数和分配给应用的CPU核数相同
  *   -读取外部文件创建RDD
  *     最小分区数：分配给应用的CPU核数和2取最小值
  */
object Spark03_Partitions_default {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过集合创建RDD默认分区数
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    rdd.saveAsTextFile("/Users/amos/BigdataLearn/10_Spark/Projects/myspark/output")

    //通过读取外部文件创建RDD默认分区数
//    val rdd: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\1.txt")
//    rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output")

    // 关闭连接
    sc.stop()
  }
}
