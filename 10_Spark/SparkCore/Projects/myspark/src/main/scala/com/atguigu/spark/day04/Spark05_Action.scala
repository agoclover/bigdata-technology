package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 行动算子
  */
object Spark05_Action {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //val rdd: RDD[Int] = sc.makeRDD(List(1,7,3,4,6,2),2)
    /*
    //reduce  对RDD中的元素进行聚合操作
    val res: Int = rdd.reduce(_+_)
    println(res)

    //collect 以数组的形式返回数据集
    val arr: Array[Int] = rdd.collect()
    arr.foreach(println)
    println("----------------------------")
    //遍历RDD中的每一个元素
    rdd.foreach(println)

    //count 获取RDD中元素的个数
    val res: Long = rdd.count()
    println(res)

    //first 返回RDD中第一个元素
    val res: Int = rdd.first()

    //take  返回由RDD前n个元素组成的数组
    val res: Array[Int] = rdd.take(3)

    //takeOrdered  返回该RDD排序后前n个元素组成的数组
    val res: Array[Int] = rdd.takeOrdered(3)
    println(res.mkString(","))

    //aggregate   对RDD中的元素进行聚合计算    先计算分区内，再计算分区间，
    //第一个参数为初始值   ，首先会给每个分区分配初始值，用初始值集合分区内计算规则  对区内元素进行聚合
    //分区间在计算的时候，也会有初始值
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //val res: Int = rdd.aggregate(0)(_+_,_+_)
    //val res: Int = rdd.aggregate(10)(_+_,_+_)
    val res: Int = rdd.fold(10)(_+_)
    println(res)

    //countByKey    计算每一个key的个数
    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))

    val res: collection.Map[Int, Long] = rdd.countByKey()
    println(res)
    */

    //save相关的算子
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    rdd.saveAsTextFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output")
    rdd.saveAsObjectFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output1")
    rdd.map((_,1)).saveAsSequenceFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output2")
    // 关闭连接
    sc.stop()
  }
}
