package com.atguigu.scala.chapter04

import scala.io.StdIn

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc: 程序流程控制
  *   顺序流程
  *   分支流程|选择流程
  *     >单分支  if
  *     >双分支  if ...else
  *     >多分支  if ...else if ...else if...
  *   循环流程
  *     >for
  *     >while
  *     >do...while
  */
object Scala01_TestIf {
  def main(args: Array[String]): Unit = {
    /*
    //单分支
    println("请输入你的年龄")
    val age: Int = StdIn.readInt()
    if(age < 18){
      println("童年")
    }

    //双分支
    println("请输入你的年龄")
    val age: Int = StdIn.readInt()
    if(age < 18){
      println("童年")
    }else{
      println("成年")
    }

    //双分支
    println("请输入你的年龄")
    val age: Int = StdIn.readInt()
    if(age < 18){
      println("童年")
    }else if(18<=age && age <=40){
      println("成年")
    }else{
      println("老年")
    }

    val age: Int = 20
    //在Scala语言中，分支有返回值，返回值的类型取决于每一个分支最后一行代码
    //如果每一个分支体最后一行代码，返回的类型不一致，以他们共同的祖先类型作为返回值类型
    val res: Any = if(age < 18){
      "童年"
    }else if(18<=age && age <=40){
      100
    }else{
      "老年"
    }
    println(res)
    */
    //在Java语言中，有三元运算符    条件表达式?值1:值2   age<18?"童年":"成年"
    //在Scala语言中，没有三元运算符   通过if ... else...
    var age = 20
    if(age < 18) "童年" else "成年"

  }
}
