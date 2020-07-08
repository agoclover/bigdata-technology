package com.atguigu.scala.chapter07

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: 可变和不可变之间的转换
  */
object Scala03_TestArray_imTomu {
  def main(args: Array[String]): Unit = {
    /*
    //定义一个不可变数组
    val arr: Array[Int] = Array(1,2,3)

    //不可变->可变
    val buffer: mutable.Buffer[Int] = arr.toBuffer

    //可变->不可变
    val newArr: Array[Int] = buffer.toArray
  */
    //多维数组
    val arr: Array[Array[Int]] = Array.ofDim[Int](2,3)
    //给二维数组元素赋值
    arr(0)(1) = 10
    //对二维数组进行遍历
    //for(i <- 0 until arr.length){
    //  for(j <- 0 until arr(i).length){
    //    println(arr(i)(j))
    //  }
    //}
    //for (i <- 0 until arr.length;j <- 0 until arr(i).length){
    //  println(arr(i)(j))
    //}

    val array: Array[Array[Array[Array[Array[Int]]]]] = Array.ofDim[Int](2,3,4,5,6)

  }
}
