package com.atguigu.myspark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/10 10:24 上午
 */
object S03_TransKV_mapValues {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(List((1,"aa"), (2,"bb"), (3,"cc")))
        .mapValues(_+"|||")
        .collect()
        .foreach(println)

    // 关闭连接
    sc.stop()
  }
}
