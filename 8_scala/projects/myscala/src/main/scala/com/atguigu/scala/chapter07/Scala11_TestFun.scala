package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 衍生集合
  */
object Scala11_TestFun {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1,2,3,4)
    //（1）获取集合的头
    //println(list.head)
    //（2）获取集合的尾（不是头的就是尾）
    //println(list.tail)
    //（3）集合最后一个数据
    //println(list.last)
    //（4）集合初始数据（不包含最后一个）
    //println(list.init)
    //（5）反转
    //println(list.reverse)
    //（6）取前（后）n个元素
    //println(list.take(2))
    //println(list.takeRight(2))
    //（7）去掉前（后）n个元素
    //println(list.drop(2))
    //println(list.dropRight(2))

    val list1 = List(1,2,3,4,5,6,7)

    val list2 = List(4,5,6,7,8,9,10,11,12,13)
    //（8）并集
    //println(list1.union(list2))
    //（9）交集
    //println(list1.intersect(list2))
    //（10）差集
    //println(list1.diff(list2))
    //println(list2.diff(list1))

    //（11）拉链  如果两个集合中元素个数不一致，在进行拉链的时候，只会将匹配的元素进行拉链
    //val newList: List[(Int, Int)] = list1.zip(list2)
    //println(newList)

    //（12）滑窗
    for (elem <- list1.sliding(3,3)) {
      println(elem)
    }


  }
}
