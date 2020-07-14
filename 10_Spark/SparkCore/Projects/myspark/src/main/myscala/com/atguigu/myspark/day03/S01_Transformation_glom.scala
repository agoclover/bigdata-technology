package com.atguigu.myspark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 9:27 上午
 */
object S01_Transformation_glom {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd :RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val newRDD: RDD[Array[Int]] = rdd.glom()
//    newRDD.foreach(arr => println(arr.mkString(",")))

    val maxRDD: RDD[Int] = newRDD.map(_.max)

    val res = maxRDD.sum()

    println(res)
    // 关闭连接
    sc.stop()
  }
}
