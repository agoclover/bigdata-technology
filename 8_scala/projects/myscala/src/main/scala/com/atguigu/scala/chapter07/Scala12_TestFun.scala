package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 	集合计算初级函数
  */
object Scala12_TestFun {
  def main(args: Array[String]): Unit = {
    val list = List(5,7,9,8,1,2,6,3,4)
    //（1）求和
    //println(list.sum)
    //（2）求乘积
    //println(list.product)
    //（3）最大值
    //println(list.max)
    //（4）最小值
    //println(list.min)
    //（5）排序
    //sorded|sortBy|sortWith
    //sorded适用于排序规则比较简单的情况
    //println(list.sorted)

    //sortBy  适用于自己指定排序规则
    //如果匿名函数，输入和输出一致，那么这个时候不能简化
    //println(list.sortBy(elem => elem))
    //println(list.sortBy(_))

    //sortWith  适用于自己指定排序规则
    //println(list.sortWith((a: Int, b: Int) => a < b))
    //println(list.sortWith((a: Int, b: Int) => a > b))
    println(list.sortWith(_ > _))

  }
}
