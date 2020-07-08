package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-reduceByKey   对RDD中相同的key的value进行聚合
  */
object Spark12_Transformation_reduceByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))
    val newRDD: RDD[(String, Int)] = rdd.reduceByKey(_+_)
    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}