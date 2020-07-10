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
 * @date 2020/7/10 11:16 上午
 */
object SCase_TopN {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD: RDD[String] = sc.textFile("/Users/amos/BigdataLearn/10_Spark/Projects/myspark/input")

    topN(RDD,3)
        .collect()
        .foreach(println)

    // 关闭连接
    sc.stop()
  }

  // p a -> (pa,1) -> (pa, n) -> (p,an) (p,an)-> groupByKey ->(p, Iterable[(a,n)])
  def topN(rdd : RDD[String], n:Int): RDD[(String, List[(String, Int)])] ={
    val arrStr: RDD[Array[String]] = rdd.map(_.split(" "))
    // p-province a-ad c-count
    arrStr.map(arr=>((arr(1), arr(4)),1))
      .reduceByKey(_+_)
      .map{case (pa,c)=>(pa._1, (pa._2,c))}
      .groupByKey()
      .mapValues(_.toList.sortBy(-1 * _._2).take(n))
  }
}
