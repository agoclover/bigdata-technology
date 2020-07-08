package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 基本属性和常用操作
  */
object Scala10_TestFun {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1,2,3,4)
    //（1）获取集合长度
    //println(list.length)
    //（2）获取集合大小
    //println(list.size)
    //（3）循环遍历
    //list.foreach(println)
    //（4）迭代器
    //for (elem <- list.iterator) {println(elem)}
    //（5）生成字符串
    //println(list.mkString(","))
    //（6）是否包含
    println(list.contains(10))

  }
}
