package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 通过集合创建RDD(内存)
  */
object Spark01_CreateRDD_mem {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_CreateRDD_mem")
    //创建SparkContext对象
    val sc: SparkContext = new SparkContext(conf)

    //通过集合创建RDD  方式1
    //val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4))

    //方式2
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    rdd.collect().foreach(println)

    //释放资源
    sc.stop()
  }
}
