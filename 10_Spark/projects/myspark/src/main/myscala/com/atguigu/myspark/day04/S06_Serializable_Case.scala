package com.atguigu.myspark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Serializable video-13</p>
 *
 * <p>
 *   1. Searcher extends Serializable
 *   2. case Searcher
 *   3. see below
 * </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/10 3:13 下午
 */
object S06_Serializable_Case {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List("hello", "atguigu", "amos", "atguigu"))
    val searcher : Searcher = new Searcher("hello")
    searcher.myFilter(rdd).foreach(println)
    // 关闭连接
    sc.stop()
  }

}
class Searcher(query:String){
  //过滤当前RDD中包含query 的元素
  def myFilter(rdd:RDD[String]): RDD[String] ={
    var q : String = query // String 本身序列化了
    rdd.filter(_.contains(query))
  }

  def isContrains(q:String): Boolean ={
    q.contains(query)
  }

  def myFilter2(rdd:RDD[String]): RDD[String] ={
    rdd.filter(isContrains)
  }
}
