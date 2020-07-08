package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 构造器参数
  */
object Scala09_TestConstructorPar {
  def main(args: Array[String]): Unit = {
    val std = new Student09("zhangsan",20)
    std.name = "zhangsan"
    //std.age = 20
  }
}

/*
class Student09(nameParam:String,ageParam:Int){
  var name:String = nameParam
  var age :Int = ageParam
}
*/
/*
//在主构造方法中，声明参数，参数前没有加任何修饰，这个时候参数相当于局部变量
class Student09(name:String,age:Int){
  def m1(): Unit ={
    println(name + ":::" + age)
  }
}
*/
//在主构造方法中，声明参数，参数前加var|val修饰，这个时候参数相当于类的属性，var可变，val不可变
/*
class Student09(var name:String,val age:Int){
  def m1(): Unit ={
    println(name + ":::" + age)
  }
}
*/
//开发常用的方式
class Student09(){
  var name:String =_
  var age:Int =_
  def this(name:String,age:Int){
    this()
    this.name = name
    this.age = age
  }
}
