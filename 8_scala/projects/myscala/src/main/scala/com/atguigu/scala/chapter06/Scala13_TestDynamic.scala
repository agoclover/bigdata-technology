package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: Scala语言属性和方法都是动态绑定
  */
object Scala13_TestDynamic {
  def main(args: Array[String]): Unit = {
    //val tea: Teacher = new Teacher
    val tea: Person = new Teacher
    println(tea.name)
    tea.hello()
    //tea.sayHi
  }
}

class Person {
  val name: String = "person"

  def hello(): Unit = {
    println("hello person")
  }
}

class Teacher extends Person {

  override val name: String = "teacher"

  override def hello(): Unit = {
    println("hello teacher")
  }
  def sayHi(): Unit ={

  }
}
