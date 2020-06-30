package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc: 函数定义
  */
object Scala02_TestFunDef {
  def main(args: Array[String]): Unit = {
    //（1）函数1：无参，无返回值
    //def f1(): Unit ={
    //  println("无参，无返回值")
    //}

    //（2）函数2：无参，有返回值
    //def f2(): Int ={
    //  println("无参，有返回值")
    //  10
    //}
    //（3）函数3：有参，无返回值
    //def f3(name:String): Unit ={
    //  println("有参，无返回值")
    //}

    //（4）函数4：有参，有返回值
    //def f4(name:String): String ={
    //  "hello:" + name
    //}

    //（5）函数5：多参，无返回值
    //def f5(name:String,age:Int): Unit ={
    //  println(name + ":::" + age)
    //}

    //（6）函数6：多参，有返回值
    def f6(name:String,age:Int): String ={
      println(name + ":::" + age)
      "abc"
    }
    println(f6("abc",20))
  }
}
