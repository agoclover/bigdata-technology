package com.atguigu.spark.day06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/13
  * Desc: 广播变量
  */
object Spark03_Broadcast {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)),3)

    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))
    //创建广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    //(key,(v1,v2))
    val resRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (k1, v1) => {
        var v3 = 0
        //for ((k2, v2) <- list) {
        for ((k2, v2) <- broadcastList.value) {
          if (k1 == k2) {
            v3 = v2
          }
        }
        (k1, (v1, v3))
      }
    }
    resRDD.foreach(println)


    // 关闭连接
    sc.stop()
  }
}
