package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 不可变List
  */
object Scala04_TestList_im {
  def main(args: Array[String]): Unit = {
    //（1）List默认为不可变集合
    //（2）创建一个List（数据有顺序，可重复）
    //val list: List[Int] = List(1,2,3)
    //（3）遍历List
    //（4）List增加数据   在Scala语言中，如果运算符方法中包含冒号，并且冒号在后，运算顺序，从右到做
    //println(list :+ 20)
    //println(list.+:(10))
    //println(10 +: list)
    //println(30 :: list)

    //（7）空集合Nil  向空集合中添加元素
    //val list1: List[Int] = 30::20::10::Nil
    //list1.foreach(println)

    //（5）集合间合并：将一个整体拆成一个一个的个体，称为扁平化
    val list1 = List(1,2,3)
    val list2 = List(4,5,6)
    //var list3 = list1 :: list2
    var list3 = list1 ::: list2
    println(list3)
    //（6）取指定数据
    println(list3(3))


  }
}
