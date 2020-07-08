package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 特质的声明以及混入
  */
object Scala16_TestTrait {
  def main(args: Array[String]): Unit = {
    val class1: PersonTrait16 = new MyClass16
  }
}

trait PersonTrait16{
  //抽象属性
  var name:String
  //抽象方法
  def m1():Unit
  //非抽象属性
  var age:Int = _
  //非抽象方法
  def m2():Unit={
    println("m2")
  }
}

//混入特质
class MyClass16 extends PersonTrait16{
  override var name: String = "xxx"

  override def m1(): Unit = {println("yyyy")}
}
