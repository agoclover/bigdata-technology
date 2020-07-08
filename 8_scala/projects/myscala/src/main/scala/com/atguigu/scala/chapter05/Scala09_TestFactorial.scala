package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 使用Scala的递归求一个整数的阶乘
  */
object Scala09_TestFactorial {
  def main(args: Array[String]): Unit = {
    println(factorial(5))
  }
  def factorial(n:Int): Int ={
    /*if(n == 1){
      1
    }else{
      n * factorial(n-1)
    }*/

    if(n == 1){ // 如果不加return, 1 返回给 if, 接着执行 n * factorial(n-1), 会导致死循环
      return 1
    }
    n * factorial(n-1)
  }
}
