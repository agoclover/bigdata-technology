package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/10
  * Desc: 属性|方法序列化案例
  */
object Spark07_TestClosure {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest")
      .setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher]))


    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello","world","scala","helloSpark"))
    val searcher: Searcher = new Searcher("hello")
    val newRDD: RDD[String] = searcher.myFilter2(rdd)
    newRDD.foreach(println)
    // 关闭连接
    sc.stop()
  }
}

//class Searcher(query:String) extends Serializable {
case class Searcher(query:String) {
//class Searcher(query:String) {
  //过滤出当前RDD中包含query的元素
  def myFilter(rdd:RDD[String]): RDD[String] ={
    var q:String = query
    rdd.filter(_.contains(q))
  }

  //判断字符串中是否包含某一个子串
  def isContains(s:String): Boolean ={
    s.contains(query)
  }

  def myFilter2(rdd:RDD[String]): RDD[String] ={
    rdd.filter(isContains)
  }
}
