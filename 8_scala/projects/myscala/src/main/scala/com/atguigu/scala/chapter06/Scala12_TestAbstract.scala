package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 抽象
  * （1）如果父类为抽象类，那么子类需要将抽象的属性和方法实现，否则子类也需声明为抽象类
  * （2）重写非抽象方法需要用override修饰，重写抽象方法则可以不加override。
  * （3）子类中调用父类的方法使用super关键字
  * （4）子类对抽象属性进行实现，父类抽象属性可以用var修饰；
  * 子类对非抽象属性重写，父类非抽象属性只支持val类型，而不支持var。
  * 因为var修饰的为可变变量，子类继承之后就可以直接使用，没有必要重写
  * （5）Scala中属性和方法都是动态绑定，而Java中只有方法为动态绑定。
  */
object Scala12_TestAbstract {
  def main(args: Array[String]): Unit = {
    val per: Person12 = new Person12 {
      override var name: String = "ff"

      override def m1(): Unit = "xxx"
    }
    per
  }
}

abstract class Person12{
  //抽象属性
  var name:String
  //抽象方法
  def m1():Unit
  //非抽象属性
  val age:Int = 10
  //非抽象方法
  def m2(): Unit ={
    println("m222222222")
  }
}

class MyClass12 extends Person12{
  var name: String = "myClass"

  //重写抽象内容则可以不加override
  def m1(): Unit = {
    println("m1111111")
  }

  //重写父类的非抽象属性
  override val age:Int = 20

  //重写非抽象内容需要用override修饰
  override def m2(): Unit = {
    println("start m22222")
    //子类中调用父类的方法使用super关键字
    super.m2()
    //不能通过super关键字访问父类的属性
    //println(name)
    println("end m22222")

  }
}
