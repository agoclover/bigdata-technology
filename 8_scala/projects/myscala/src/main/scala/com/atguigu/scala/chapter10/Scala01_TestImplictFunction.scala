package com.atguigu.scala.chapter10

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc: 隐式转换---隐式函数
  *   动态的扩展类的功能
  *   在编译器第一次编译失败的时候，底层会尝试去当前作用域中找能让程序编译通过的代码。
  */
object Scala01_TestImplictFunction {
  /*
  //将Int类型转换为MyRichInt   函数用implicit修饰，那么这个函数被称为隐式函数
  implicit def convert(a:Int): MyRichInt ={
    new MyRichInt(a)
  }*/

  /*
  implicit def convert1(a:Int): MyRichInt ={
    new MyRichInt(a)
  }
  */

  implicit class MyRichInt(self:Int){
    def myMax(n:Int): Unit ={
      if(self > n) self else n
    }
  }

  def main(args: Array[String]): Unit = {
    println(10.myMax(6))
  }
}
