package com.atguigu.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/6
  * Desc:  单词个数的统计
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建SparkContext上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //读取外部文件，创建RDD
    val lineRDD: RDD[String] = sc.textFile("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input")

    //对读取到的一行数据进行扁平映射
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    //对上面的结构进行转换 (word,1)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    //按照相同的key对value进行聚合
    val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    //触发行动操作
    val res: Array[(String, Int)] = resRDD.collect()

    res.foreach(println)
    /*
    //创建配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("yarn").setAppName("WordCount")
    //创建SparkContext上下文对象
    val sc: SparkContext = new SparkContext(conf)

    //一行代码搞定上面操作  saveAsTextFile和collect一样，也是行动算子，也会触发操作
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))
    */

    //释放资源
    sc.stop()
  }
}
