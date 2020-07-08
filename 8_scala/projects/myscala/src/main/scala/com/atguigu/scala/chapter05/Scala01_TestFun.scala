package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc:  函数基本语法以及和方法的区别
  *   -函数：将完成某一个功能的代码块封装在一起
  *   -方法
  *     *定义在类下的函数称之为方法
  *     *方法支持重载和重写，但是函数不支持
  *
  */
object Scala01_TestFun {
  def main(args: Array[String]): Unit = {
    //函数的调用
    sayHi("fangfang")
  }
  //定义一个函数，将输入的字符串打印输出
  def sayHi(name:String): Unit ={
    import java.util.Date
    new Date
    println("hello-->" + name)
  }
}
