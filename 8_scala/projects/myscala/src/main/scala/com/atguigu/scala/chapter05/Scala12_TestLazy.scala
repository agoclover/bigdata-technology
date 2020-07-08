package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 惰性函数
  */
object Scala12_TestLazy {
  def main(args: Array[String]): Unit = {
    lazy val res = sum(10, 30)
    println("----------------")
    println("res=" + res)
  }

  def sum(n1: Int, n2: Int): Int = {
    println("sum被执行。。。")
    n1 + n2
  }

}
