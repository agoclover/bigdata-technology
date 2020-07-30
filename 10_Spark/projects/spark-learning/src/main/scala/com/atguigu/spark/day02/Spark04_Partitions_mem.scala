package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc:通过集合创建RDD分区规则
  * def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
  * (0 until numSlices).iterator.map { i =>
  * val start = ((i * length) / numSlices).toInt
  * val end = (((i + 1) * length) / numSlices).toInt
  * (start, end)
  * }
  * }
  */
object Spark04_Partitions_mem {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //4个数据，设置4个分区   0分区->1,  1分区->2, 2分区->3, 3分区->4
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
    //rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output")

    //4个数据，设置3个分区  0分区->1,  1分区->2, 2分区->3,4
    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),3)
    //rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output")

    //5个数据，设置3个分区  0分区->1  1分区->2,3   2分区->4,5
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)
    rdd.saveAsTextFile("/Users/amos/BigdataLearn/10_Spark/Projects/myspark/output")

    // 关闭连接
    sc.stop()
  }
}
