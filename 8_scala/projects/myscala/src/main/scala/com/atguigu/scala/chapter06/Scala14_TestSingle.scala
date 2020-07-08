package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 单例实现
  *   -懒汉式
  *   -饿汉式
  *   -构造方法私有化
  *   -提供公开的静态的属性
  *   -提供公开的静态的方法获取单例对象
  */
object Scala14_TestSingle {
  def main(args: Array[String]): Unit = {
    val std1 = Student14.getInstance()
    val std2 = Student14.getInstance()
    println(std1.eq(std2))
    println(std1)
    println(std2)
  }
}

//主构造方法私有化
/*
懒汉式
object Student14{
  var std:Student14 = null
  def getInstance():Student14={
    if(std == null){
      std = new Student14
    }
    std
  }
}*/
//饿汉式
object Student14{
  private var std:Student14 = new Student14
  def getInstance():Student14={
    std
  }
}
class Student14 private(){

}
