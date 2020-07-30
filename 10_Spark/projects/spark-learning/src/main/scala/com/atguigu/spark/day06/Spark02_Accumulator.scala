package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/13
  * Desc: 需求：自定义累加器，统计RDD中首字母为“H”的单词以及出现的次数。 (word,count)
  */
object Spark02_Accumulator {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val wordRDD: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hehe", "Hello", "Hello", "Spark", "Spark"))

    //创建累加器对象
    val myac = new MyAccumulator
    //注册累加器
    sc.register(myac)
    //使用累加器
    wordRDD.foreach(
      word=>{
        myac.add(word)
      }
    )
    //获取累加结果
    println(myac.value)
    // 关闭连接
    sc.stop()
  }
}
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]]{
  //定义一个Map集合存放结果单词以及出现的次数
  var map = mutable.Map[String,Int]()

  //判断是否为初始状态
  override def isZero: Boolean = !map.isEmpty

  //拷贝副本
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val myac = new MyAccumulator
    myac.map = this.map
    myac
  }

  //恢复到初始状态
  override def reset(): Unit = {
    map.clear()
  }

  //向累加器中添加数据
  override def add(word: String): Unit = {
    //判断当前单词是否以"H"开头
    if(word.startsWith("H")){
      map(word) = map.getOrElse(word,0) + 1
    }
  }

  //多个Task数据合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    var map1 = this.map
    var map2 = other.value
    map = map1.foldLeft(map2)((mm,kv)=>{
      val k: String = kv._1
      val v: Int = kv._2
      mm(k) = mm.getOrElse(k,0) + v
      mm
    })
  }

  //获取累加器的值
  override def value: mutable.Map[String, Int] = map
}
