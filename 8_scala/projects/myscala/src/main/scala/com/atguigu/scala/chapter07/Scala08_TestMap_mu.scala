package com.atguigu.scala.chapter07

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 可变map
  */
object Scala08_TestMap_mu {
  def main(args: Array[String]): Unit = {
    //（1）创建可变集合
    val map: mutable.Map[String, Int] = mutable.Map("a"->1,"b"->2)
    //（2）打印集合
    //map.foreach(println)
    //（3）向集合增加数据
    map.put("c",3)
    //（4）删除数据
    //map.remove("a")
    //（5）修改数据
    //map.update("c",30)
    //map.update("d",30)

    map("d")=30

    map.foreach(println)
  }
}
