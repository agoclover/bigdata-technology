package com.atguigu.scala.chapter07

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc: 合并两个map集合
  */
object Scala15_TestMergeMap {
  def main(args: Array[String]): Unit = {
    // 两个Map的数据合并
    val map1 = mutable.Map("a"->1, "b"->2, "c"->3)
    val map2 = mutable.Map("a"->4, "b"->5, "d"->6)

    //fold--foldLeft--foldRight
    val res: mutable.Map[String, Int] = map1.foldLeft(map2)((mm, kv) => {
      val k = kv._1 //c
      val v = kv._2 //3
      mm(k) = mm.getOrElse(k, 0) + v
      mm
    })
    println(res)
    //练习：  map2.foldLeft(map1)
  }
}
