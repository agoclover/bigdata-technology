package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 特质叠加
  */
object Scala18_TestTrait {
  def main(args: Array[String]): Unit = {

  }
}
trait Trait18A{
  def m1():Unit={
    println("18Am1")
  }
}
trait Trait18B{
  def m1():Unit={
    println("18Bm1")
  }
}

class MyClass18 extends Trait18A with Trait18B{
  override def m1(): Unit = {
    println("xxxx")
  }
}
