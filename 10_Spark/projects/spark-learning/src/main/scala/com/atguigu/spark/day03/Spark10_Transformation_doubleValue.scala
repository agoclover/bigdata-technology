package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-双value类型
  */
object Spark10_Transformation_doubleValue {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(4,5,6,7),3)
    //合集  union
    //val newRDD: RDD[Int] = rdd1.union(rdd2)

    //差集  diff--->subtract
    //val newRDD: RDD[Int] = rdd1.subtract(rdd2)

    //交集  intersect--->intersection
    //val newRDD: RDD[Int] = rdd1.intersection(rdd2)

    //拉链  zip
    //要求每个分区中元素的个数相同  ：Can only zip RDDs with same number of elements in each partition
    //要求分区数相同 ：Can't zip RDDs with unequal numbers of partitions
    val newRDD: RDD[(Int, Int)] = rdd1.zip(rdd2)

    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
