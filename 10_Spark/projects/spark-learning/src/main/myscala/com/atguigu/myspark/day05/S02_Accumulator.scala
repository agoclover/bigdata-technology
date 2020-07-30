package com.atguigu.myspark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * <p>Accumulator</p>
 *
 * <p>累加器</p>
 *
 * @author Zhang Chao
 * @version spark_day6
 * @date 2020/7/13 10:01 上午
 */
object S02_Accumulator {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD: RDD[String] = sc.makeRDD(List("HiAmos", "Hello", "HiAmos", "Hello", "Hello", "Spark", "Spark"))

    val myac = new MyAccumulator

    sc.register(myac)

    RDD.foreach(word =>myac.add(word))

    println(myac.value)

    // 关闭连接
    sc.stop()

  }
}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

  private var map: mutable.Map[String, Int] = mutable.Map[String, Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    var myac = new MyAccumulator
    myac.map = this.map
    myac
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(v.startsWith("H")){
      map(v) = map.getOrElse(v, 0) + 1 //这里可以这样做的原因是 map 是 var
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1: mutable.Map[String, Int] = this.value
    val map2: mutable.Map[String, Int] = other.value
    this.map = map1.foldLeft(map2)((m2, kv)=>{
      val k: String = kv._1
      val v: Int = kv._2
      m2(k) = m2.getOrElse(k, 0) + v
      m2
    })
  }

  override def value: mutable.Map[String, Int] = map
}