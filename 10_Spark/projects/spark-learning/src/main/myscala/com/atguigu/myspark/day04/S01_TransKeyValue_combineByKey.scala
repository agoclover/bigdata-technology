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
 * @date 2020/7/10 8:42 上午
 */
object S01_TransKeyValue_combineByKey {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 88), ("b", 95), ("a", 91),
      ("b", 93), ("a", 95), ("b", 98)))

    // 通过 combineByKey - map
    /*
    先将 v 转换为新的结构 C;
    再对前一个 C 和后一个 v 进行分组内的聚合计算;
    最后对不同分区间的 C 进行聚合运算.
     */
    RDD.combineByKey((_,1),
      (vc:(Int,Int),v:Int)=>(vc._1+v, vc._2+1),
      (vc1:(Int,Int),vc2:(Int,Int))=>(vc1._1+vc2._1,vc1._2+vc2._2))
        .map{case (k, vc)=>(k, (vc._1.toDouble/vc._2).formatted("%.2f"))}
        .collect()
        .foreach(println)

    println("----")

    // 通过 map - reduceByKey - map
    RDD.map{case (k, v) => (k, (v, 1))}
      .reduceByKey((v1, v2)=>(v1._1+v2._1,v1._2+v2._2))
      .map{case (k, (v, c))=>(k,(v.toDouble/c).formatted("%.2f"))}
      .collect()
      .foreach(println)

    // 关闭连接
    sc.stop()
  }
}
