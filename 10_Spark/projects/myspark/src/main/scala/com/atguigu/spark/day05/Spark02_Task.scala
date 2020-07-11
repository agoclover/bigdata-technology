package com.atguigu.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/6
  * Desc:  Spark的Job调度
  *   相关重要的概念
  *     集群
  *       Standalone，Yarn
  *     应用
  *       我们编写的一个Spark程序，一般创建一个SparkContext，表示创建了一个应用
  *
  *      一个集群中可以有多个应用
  *    Job
  *       每次触发行动操作，都会提交一个Job
  *       一个Spark应用可以有多个Job
  *
  *    Stage
  *       阶段，根据当前Job中宽依赖的数量来划分阶段
  *       阶段的数量 = 宽依赖的数量 +１
  *
  *    Task
  *       任务，每个阶段由多个Task组成
  *       每个阶段最后一个RDD的分区的个数就是当前阶段的任务数
  *
  *
  */
object Spark02_Task {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    //创建SparkContext上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    val lineRDD: RDD[String] = sc.makeRDD(List("hello world","hello spark"))


    //对读取到的一行数据进行扁平映射
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))


    //对上面的结构进行转换 (word,1)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    //按照相同的key对value进行聚合
    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    resRDD.collect()

    resRDD.saveAsTextFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\output")

    Thread.sleep(1000000)

    //释放资源
    sc.stop()
  }
}
