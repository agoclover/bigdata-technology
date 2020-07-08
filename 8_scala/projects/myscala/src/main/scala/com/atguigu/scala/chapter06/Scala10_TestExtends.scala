package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 继承
  *   存在继承关系子类对象的构建过程
  *     -分配空间
  *     -递归的构造父类对象
  *     -构造子类对象
  */
object Scala10_TestExtends {
  def main(args: Array[String]): Unit = {
    val zs = new Student10("zs",20,"1000")
    println(zs.name)
  }
}
class Person10{
  println("父类的主构造方法被执行了")
  var name:String = _
  var age:Int = _
  def this(name:String,age:Int){
    this()
    println("父类的辅助构造方法被执行了")
    this.name = name
    this.age = age
  }
}

class Student10(name:String,age:Int) extends Person10(name,age){
  println("子类的主构造方法被执行了")
  var stdNo:String = _
  def this(name:String,age:Int,stdNo:String){
    this(name,age)
    println("子类的主构造方法被执行了")
    this.stdNo = stdNo
  }
}

