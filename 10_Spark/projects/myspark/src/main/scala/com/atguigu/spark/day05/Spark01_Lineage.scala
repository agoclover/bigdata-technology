package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/6
  * Desc:  查看RDD的血缘关系以及依赖
  */
object Spark01_Lineage {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建SparkContext上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lineRDD: RDD[String] = sc.makeRDD(List("hello world","hello spark"))

    //查看rdd的血缘关系
    println(lineRDD.toDebugString)
    println(lineRDD.dependencies)
    println("-----------------------------------")

    //对读取到的一行数据进行扁平映射
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    println(wordRDD.toDebugString)
    println(wordRDD.dependencies)
    println("-----------------------------------")


    //对上面的结构进行转换 (word,1)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    println(mapRDD.toDebugString)
    println(mapRDD.dependencies)
    println("-----------------------------------")

    //按照相同的key对value进行聚合
    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resRDD.toDebugString)
    println(resRDD.dependencies)
    println("-----------------------------------")

    //释放资源
    sc.stop()
  }
}
