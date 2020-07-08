package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-sortBy 对RDD中的元素进行排序
  */
object Spark09_Transformation_sortBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //val rdd: RDD[Int] = sc.makeRDD(List(1,2,6,3,4,8,5),3)

    //对RDD中的元素进行排序  默认升序
    //val newRDD: RDD[Int] = rdd.sortBy(ele=>ele)
    //val newRDD: RDD[Int] = rdd.sortBy(ele=>ele,false)
    //val newRDD: RDD[Int] = rdd.sortBy(ele=> -ele)

    //val rdd: RDD[String] = sc.makeRDD(List("1","11","2","6","3"),3)
    //val newRDD: RDD[String] = rdd.sortBy(ele=>ele)
    //val newRDD: RDD[String] = rdd.sortBy(_.toInt)

    val rdd3: RDD[(Int, Int)] = sc.makeRDD(List((2, 1), (1, 2), (1, 1), (2, 2)))
    //先按照元组第一个元素进行排序，再按照元组第二个元素进行排序
    val newRDD: RDD[(Int, Int)] = rdd3.sortBy(t=>t,false)
    newRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
