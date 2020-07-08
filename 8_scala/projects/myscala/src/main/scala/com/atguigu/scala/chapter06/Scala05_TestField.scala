package com.atguigu.scala.chapter06

import scala.beans.BeanProperty

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 类的属性
  */
object Scala05_TestField {
  def main(args: Array[String]): Unit = {
    val std: Student05 = new Student05
    println(std.name)
    println(std.age)
    println(std.sex)
    //std.name = "lisi"
    //println(std.name)
  }
}
class Student05{
  //Scala的属性默认是遵循封装原则，默认会自动的将属性编译为私有，并且提供公开的获取以及设置属性值的方法
  //为了去兼容一些框架，可以通过@BeanProperty注解生成符合JavaBean规范的get|set方法
  //@BeanProperty
  //var name:String = "zhangsan"

  //Scala语言中属性的默认修饰符是public，但是不能显示声明public
  //private var name:String = "zhangsan"

  var name:String = "zhangli"
  //在声明属性的时候，通过_给属性赋默认值
  var age:Int = _
  var sex:String = _
}
