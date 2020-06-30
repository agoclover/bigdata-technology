package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc:  类和对象
  *   -对象
  *     在自然界中，只要是客观存在的事物都是对象(万物皆对象)
  *   -类
  *     *对大量对象共性的抽象
  *     *在Java|Scala语言中，类是创建对象的模板
  *     *类是客观事物在人脑中的主观反映
  *
  *  package 包名;
  *  import 导包；
  *  class 类 extends 父类 implements 接口{
  *    属性；有什么
  *    方法；能做什么
  *    构造器
  *  }
  */
object Scala01_TestObject {
  def main(args: Array[String]): Unit = {
    //创建学生对象
    val std:Student01 = new Student01
    //调用对象的属性和方法
    println(std.name)
    println(std.age)
    std.study()
    std.eat()
  }
}

class Student01{
  //默认情况下，属性必须赋值
  var name:String="atguigu"
  var age:Int = 100

  def study(): Unit ={
    println("study。。。。")
  }
  def eat(): Unit ={
    println("eat.....")
  }
}
