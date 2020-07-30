package com.atguigu.myspark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/10 10:28 上午
 */
object S04_TransKV_join {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    val RDD2: RDD[(Int, Int)] = sc.makeRDD(List((1, 4), (2, 5), (4, 6)))

    RDD1.join(RDD2)
      .collect
      .foreach(println)

    // 关闭连接
    sc.stop()
  }
}
