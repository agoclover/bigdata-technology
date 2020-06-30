package com.atguigu.scala.chapter01

/**
  * Author: Felix
  * Date: 2020/6/28
  * Desc: 使用idea开发的第一个程序
  * -伴生类和伴生对象的说明
  *   在Scala中没有static关键字，通过伴生对象模拟static的功能
  *   语法：object Scala01_HelloScala
  *   声明伴生对象，伴随类产生的对象。
  *   默认情况下，伴生类可以不用显示声明，会自动的生成和伴生对象同名的类，也可以显示声明。
  *   其实严格讲，Scala01_HelloScala$不是伴生对象，是伴生对象所属类。
  *   真正的伴生对象是在伴生对象所属类中定义的单例对象。
  *   注意：以后在Scala中，只要是静态的内容都放到伴生对象中声明
  *
  * -def main(args:Array[String]):Unit ={}
  *   *主方法定义
  *   *在Scala中，通过def关键字声明方法
  *   *main是方法的名字
  *   *(args:Array[String]) 参数列表
  *       >在声明参数的时候，相当于定义了一个局部变量
  *       >scala中定义变量的语法    变量名:数据类型
  *       >在java语言中，字符串数据String[]   ;在Scala中，Array表示数组类型，[]表示泛型。
  *   * Unit 方法的返回类型
  *       >在方法的参数和返回值类型类型之间需要用:分隔
  *       >Unit相当于Java中的void关键字，表示返回值为空
  *   *在方法声明和方法体之间用等号连接
  *   *println("hello Scala")  向控制台打印输出字符串
  *   *在scala语言中，语句结束了，可以不加分号
  */
object Scala01_HelloScala {
  def main(args:Array[String]):Unit = {
    println("hello Scala")
  }
}
//伴生类
//class Scala01_HelloScala{}

/*
object Student{
  var bzr:String ="fangfang"
}
class Student{
  var name:String= "XXX"
  var age:Int = 10
}*/


