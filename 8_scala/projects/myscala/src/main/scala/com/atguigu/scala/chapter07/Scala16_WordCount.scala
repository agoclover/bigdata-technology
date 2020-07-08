package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc:
  *   单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
  */
object Scala16_WordCount {
  def main(args: Array[String]): Unit = {
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")

    //对集合中的元素进行扁平映射
    val flatMapList: List[String] = stringList.flatMap(_.split(" "))

    //将相同的单词放到一组
    val groupMap: Map[String, List[String]] = flatMapList.groupBy(elem=>elem)

    //转换结构，对单词次数进行统计
    val countMap: Map[String, Int] = groupMap.map(kv=>(kv._1,kv._2.size))

    //对单词次数进行降序排序   map没有sort，需要将其转换为list进行排序
    //val sortList: List[(String, Int)] = countMap.toList.sortBy(-_._2)
    val sortList: List[(String, Int)] = countMap.toList.sortWith(_._2 > _._2)

    //对排序后的内容取前三
    val resList: List[(String, Int)] = sortList.take(3)
    println(resList)

    // 第一种方式（不通用）
    val tupleList = List(("Hello Scala Spark World ", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    println(tupleList.map(kv => (kv._1 + " ") * kv._2))
  }
}
