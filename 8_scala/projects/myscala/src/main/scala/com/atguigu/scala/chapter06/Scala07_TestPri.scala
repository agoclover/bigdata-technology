package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 
  */
object Scala07_TestPri {
  def main(args: Array[String]): Unit = {
    val per = new Person06
    //println(per.stdNo)
    //println(per.name)
    println(per.sex)
    println(per.age)
  }
}

class Student07 extends Person06{
  def m222(): Unit ={
    //println(stdNo)
    println(name)
    println(sex)
    println(age)
  }
}