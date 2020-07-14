package com.atguigu.myspark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Lineage</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/11 8:41 上午
 */
object S01_Lineage {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD: RDD[String] = sc.makeRDD(List("aaa", "bbb", "ccc"))
    println("------")
    println(RDD.toDebugString)
    println("---")
    println(RDD.dependencies)
    println("------")

    val RDD1 = RDD.map((_, 1))
    println("------")
    println(RDD1.toDebugString)
    println("---")
    println(RDD1.dependencies)
    println("------")


    val RDD2 = RDD1.reduceByKey(_ + _)
    println("------")
    println(RDD2.toDebugString)
    println("---")
    println(RDD2.dependencies)
    println("------")

    RDD2.collect()
        .foreach(println)

    // 关闭连接
    sc.stop()
  }
}
