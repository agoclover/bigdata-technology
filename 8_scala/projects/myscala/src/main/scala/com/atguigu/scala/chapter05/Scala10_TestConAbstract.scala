package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc:
  *   控制抽象
  *     -值调用
  *       把计算后的值传递过去
  *     -名调用
  *       将代码块传递过去
  *    所以我们现在知道一般参数有三种类型: 值 Int, 函数 Int => Int, 代码块 => Int
  *     当然, 其实前两种是一种.
  */
object Scala10_TestConAbstract {
  def main(args: Array[String]): Unit = {

    /*
    //值调用   把计算后的值传递过去
    def foo(a: Int):Unit = {
      println(a)
      println(a)
    }
    def f(): Int ={
      println("f....")
      1
    }
    foo(f)
*/
    //名调用   传递的是代码块
    //Int\String\函数  参数列表=>返回值\代码块
    //def foo(a: Int):Unit = {
    def foo(a: =>Int):Unit = {
      println(a)
      println(a)
    }
    def f(): Int ={
      println("f....")
      1
    }
    foo(f)
  }
}
