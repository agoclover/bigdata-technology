package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 对RDD中的元素进行映射
  */
object Spark06_Transformation_map {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过集合创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    println("分区数："  + rdd.partitions.size)
    println("~~~~~~~~~~~~~~~~~~~~~~~")
    val newRDD: RDD[Int] = rdd.map(_*2)
    println("分区数："  + newRDD.partitions.size)
    newRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
