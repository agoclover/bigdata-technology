package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/11
  * Desc: RDD的检查点
  */
object Spark04_Checkpoint {
  def main(args: Array[String]): Unit = {

    // 设置访问HDFS集群的用户名
    System.setProperty("HADOOP_USER_NAME","atguigu")

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //设置检查点存储目录
    //sc.setCheckpointDir("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\cp")
    sc.setCheckpointDir("hdfs://hadoop202:8020/cp")

    //创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello world","hello Spark"))

    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Long)] = flatMapRDD.map(
      word => {
        (word, System.currentTimeMillis())
      }
    )
    println(mapRDD.toDebugString)

    //为了保险起见，在设置检查点之前，会从血缘关系开始执行一次，一般我们为了提高效率，检查点 + 缓存配合使用
    mapRDD.cache()
    //给RDD设置检查点
    mapRDD.checkpoint()
    mapRDD.collect().foreach(println)

    println("~~~~~~~~~~~~~~~~~~~~~~~~~")

    println(mapRDD.toDebugString)
    mapRDD.collect().foreach(println)

    Thread.sleep(1000000)
    // 关闭连接
    sc.stop()
  }
}
