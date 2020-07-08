package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/8
  * Desc: 通过groupBy算子 实现wordCount功能
  */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    //普通版实现方案1
    //创建RDD
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    //对RDD中的元素进行扁平映射
    val wordRDD: RDD[String] = strRDD.flatMap(_.split(" "))

    //对RDD中元素进行结构的转换  (word,1)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))

    //对上面的RDD按照相同的单词进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(t=>t._1)

    //对RDD进行结构的转换
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }
    resRDD.collect().foreach(println)

    //普通版实现方案2
    val strRDD: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark", "Hello World"))
    //对RDD中的元素进行扁平映射
    val wordRDD: RDD[String] = strRDD.flatMap(_.split(" "))
    //将相同的单词放到一组
    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word=>word)
    //对RDD进行结构的转化，获取单词出现的次数
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }
    resRDD.collect().foreach(println)


    //复杂版实现方案1    "Hello Scala " * 2
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))

    //对RDD结构进行转换，将元组转换字符串
    val newRDD: RDD[String] = rdd.map {
      case (wordStr, count) => {
        (wordStr + " ") * count
      }
    }
    //扁平化
    val flatMapRDD: RDD[String] = newRDD.flatMap(_.split(" "))
    //映射
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_,1))
    //分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)
    //映射
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.size)
      }
    }

    resRDD.collect().foreach(println)
*/
    //复杂版实现方案1
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))

    //对RDD中元素进行扁平映射  ("Hello Scala", 2)==>(Hello,2),(Scala,2)
    val newRDD: RDD[(String, Int)] = rdd.flatMap {
      case (wordStr, count) => {
        wordStr.split(" ").map((_, count))
      }
    }

    //对RDD中的元素按照元组的第一个元素进行分组  (Hello,CompactBuffer((Hello,2), (Hello,3), (Hello,2)))
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newRDD.groupBy(_._1)

    //对RDD中的元素进行结构的转换
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (word, datas) => {
        (word, datas.map(_._2).sum)
      }
    }

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
