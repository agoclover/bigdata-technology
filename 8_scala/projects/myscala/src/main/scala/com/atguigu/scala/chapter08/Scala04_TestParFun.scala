package com.atguigu.scala.chapter08

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc:  偏函数
  */
object Scala04_TestParFun {
  def main(args: Array[String]): Unit = {
    //将该List(1,2,3,4,5,6,"test")中的Int类型的元素加一，并去掉字符串。
    val list = List(1,2,3,4,5,6,"test")
   /* val newList: List[AnyVal] = list.map {
      case a: Int => a + 1
      case _ =>
    }
    println(newList.filter(_.isInstanceOf[Int]))
    */

    //list.map{case elem:Int=>elem + 1}.foreach(println)
    list.collect{case elem:Int=>elem + 1}.foreach(println)
  }
}
