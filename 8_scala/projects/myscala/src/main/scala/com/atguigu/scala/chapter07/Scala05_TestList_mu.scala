package com.atguigu.scala.chapter07

import scala.collection.mutable.ListBuffer

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 可变List
  */
object Scala05_TestList_mu {
  def main(args: Array[String]): Unit = {
    //创建集合对象
    //val list: ListBuffer[Int] = new ListBuffer[Int]()
    val list: ListBuffer[Int] = ListBuffer(1,2,3)

    //获取集合元素
    //println(list(1))

    //对集合中元素进行遍历
    //list.foreach(println)
    //向集合中添加元素
    //list.append(10)

    //修改集合中的元素
    //list(0) = 10
    //list.update(0,20)

    //删除集合中的元素
    list.remove(0)
    list.foreach(println)
  }
}
