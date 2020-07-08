package com.atguigu.scala.chapter10

import com.atguigu.scala.chapter10.TestTransform.Teacher

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc:  隐式转换---隐式参数
  */
object Scala02_TestImplicitParam {
  implicit var defaultName:String = "atguigu"

  //implicit var defaultName111:String = "bigdata0317"

  def sayHi(implicit name:String): Unit ={
    println(name)
  }
  //隐式参数
  def sayHi2(implicit name:String="aaa"): Unit ={
    println(name)
  }
  def main(args: Array[String]): Unit = {
    sayHi2("bbb")
  }
}

//（2）如果第一条规则查找隐式实体失败，会继续在隐式参数的类型的作用域里查找。类型的作用域是指与该类型相关联的全部伴生模块，
object TestTransform extends PersonTrait {
  def main(args: Array[String]): Unit = {
    //（1）首先会在当前代码作用域下查找隐式实体
    val teacher = new Teacher()
    teacher.eat()
    teacher.say()
  }

  class Teacher {
    def eat(): Unit = {
      println("eat...")
    }
  }
}

trait PersonTrait {

}

object PersonTrait {
  // 隐式类 : 类型1 => 类型2
  implicit class Person5(user:Teacher) {
    def say(): Unit = {
      println("say...")
    }
  }
}

