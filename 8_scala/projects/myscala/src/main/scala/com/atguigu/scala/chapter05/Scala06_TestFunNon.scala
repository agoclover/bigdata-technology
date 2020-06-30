package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 函数作为参数进行传递
  */
object Scala06_TestFunNon {
  def main(args: Array[String]): Unit = {

    //定义一个对数组中元素进行处理的函数
    def arrOp(arr:Array[Int],op:(Int)=>Int) ={
      for (elem <- arr) yield {
        op(elem)
      }
    }

    //调用函数
    val ints: Array[Int] = arrOp(Array(1,2,3,4),_*3)
    for (elem <- ints) {
      println(elem)
    }
    //arrOp(Array(1,2,3,4),elem=>elem*2)
    //arrOp(Array(1,2,3,4),(elem:Int)=>{elem*2})

  }
}
