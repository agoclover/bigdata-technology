package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc:  伴生对象的apply方法
  *   Java中对象的创建方式
  *     -new
  *     -工厂模式
  *     -反射
  *     -克隆
  *     -序列化
  *   Scala中对象创建方式
  *     -new 构造器
  *     -通过伴生对象的apply
  */
object Scala15_TestApply {
  def main(args: Array[String]): Unit = {
    val std = Student15("zhangsan")
    println(std.name)
  }
}

object Student15{
  def apply(): String = new String("xxxx")
  def apply(name:String): Student15 = new Student15(name)
  def apply(name:String,age:Int): Student15 = new Student15(name)
}
class Student15 private(var name:String){

}
