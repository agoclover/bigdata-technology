package com.atguigu.scala.chapter02

import scala.io.StdIn

/**
  * Author: Felix
  * Date: 2020/6/28
  * Desc: 获取用户在键盘上的输入
  */
object Scala05_TestStdIn {
  def main(args: Array[String]): Unit = {
    println("请输入你的名字")
    val name: String = StdIn.readLine()
    println("请输入你的收入")
    val money: Int = StdIn.readInt()

    println(s"恭喜${name}，加入珍爱网")

  }
}
