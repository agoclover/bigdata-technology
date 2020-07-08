package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 
  */
object Scala08_TestFunReturn {
  def main(args: Array[String]): Unit = {
    def f1(): (Int)=>Int ={
      var a:Int = 10
      def f2(b:Int): Int={
        a + b
      }
      f2 _
    }

    //内层函数访问外层函数的局部变量，会自动延长外层函数局部变量的生命周期，与内层函数形成一个闭合的效果，
    //我们称之为闭包       闭包 =  内层函数 + 外层函数的局部变量
    //val ff: Int => Int = f1()
    //println(ff(30))
    println(f1()(20))

    //柯里化：将一个参数列表中的多个参数拆分为多个参数列表
    // 让参数列表中的每一个参数意义更明确，简化函数的嵌套定义
    // 简化函数嵌套的编写
    def f5(a:Int)(b:Int): Int ={
      a + b
    }

    println(f5(10)(20))
  }
}
