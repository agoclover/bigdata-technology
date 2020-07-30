package com.atguigu.spark.day08

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: 通过RDD方式实现求平均年龄
  */
object Spark01_AvgRDD {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val res: (Int, Int) = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40))).map {
      case (name, age) => {
        (age, 1)
      }
    }.reduce(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    println(res._1 / res._2)

    // 关闭连接
    sc.stop()
  }
}
