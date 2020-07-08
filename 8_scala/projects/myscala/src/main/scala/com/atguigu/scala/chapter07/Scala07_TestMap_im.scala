package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 
  */
object Scala07_TestMap_im {
  def main(args: Array[String]): Unit = {
    //（1）创建不可变集合Map
    val map: Map[String, Int] = Map("a"->1,"b"->2,"c"->3)
    //（2）循环打印
    //对key进行遍历
    //for (elem <- map.keys) {
    //  println(elem)
    //}
    //对value进行遍历
    //for (elem <- map.values) {
    //  println(elem)
    //}
    //（3）访问数据
    //（4）如果key不存在，返回0
    //根据key获取value
    //val ff: Option[Int] = map.get("a")
    //println(map.get("a"))//Some(1)
    //println(map.get("d"))//None

    //println(map.get("a").getOrElse(88))
    //println(map.getOrElse("a",0))

  }
}
