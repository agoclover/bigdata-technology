package com.atguigu.scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 可变数组
  */
object Scala02_TestArray_mu {
  def main(args: Array[String]): Unit = {
    //创建数组对象
    //val arr: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val arr: ArrayBuffer[Int] = ArrayBuffer(1,2,3)
    //访问数组中的元素
    //println(arr(1))

    //对数组元素进行遍历
    //arr.foreach(println)
    println("-----------")

    //对集合元素进行相加
    //建议：在操作集合的时候，不可变用符号，可变用方法
    arr.append(10)

    //修改集合元素
    //arr(0) = 20
    //arr.update(0,66)

    //删除集合元素
    //arr.-(1)
    //arr.remove(0,2)
    //arr.foreach(println)

  }
}
