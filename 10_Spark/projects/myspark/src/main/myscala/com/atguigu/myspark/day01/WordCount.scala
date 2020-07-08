package com.atguigu.myspark.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/6 7:43 下午
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // calculate
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(args(1))

    // print the result
//     res.foreach(println)

    // close the SparkContext
    sc.stop()
  }
}
