package com.atguigu.myspark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 3:12 下午
 */
object S07_Transformation_doubleValue {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val RDD2 = sc.makeRDD(List(2, 3, 4, 5), 2)

    // union
    val resUnion = RDD1.union(RDD2)
    resUnion.collect.foreach(println)
    println("----")

    // intersect
    val resIntersection = RDD1.intersection(RDD2)
    resIntersection.collect().foreach(println)
    println("----")

    // difference-set
    val resDifference = RDD1.subtract(RDD2)
    resDifference.collect().foreach(println)
    println("----")

    // zip
    /*
    1234|2, 23456|2
    Can only zip RDDs with same number of elements in each partition
    1234|2, 2345|3
    Can't zip RDDs with unequal numbers of partitions: List(2, 3)
     */
    val resZip = RDD1.zip(RDD2)
    resZip.collect().foreach(println)
    println("----")

    // 关闭连接
    sc.stop()
  }
}
