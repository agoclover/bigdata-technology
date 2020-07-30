package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/11
  * Desc: RDD的缓存
  */
object Spark03_Cache {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello world","hello Spark"))

    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatMapRDD.map(
      word => {
        println("--------------------------")
        (word, 1)
      }
    )
    println(mapRDD.toDebugString)
    //不管是cache方法还是persist方法，不管存储级别是内存还是磁盘，都是临时的，程序结束后，缓存的内容都会清除
    //mapRDD.cache()
    //mapRDD.persist()
    mapRDD.collect()
    println("~~~~~~~~~~~~~~~~~~~~~~~~~")
    println(mapRDD.toDebugString)
    mapRDD.collect()

    //释放缓存
    //mapRDD.unpersist()
    Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
