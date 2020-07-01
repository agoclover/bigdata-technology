package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 构造器
  *   构造器作用
  *     构建对象
  *     初始化属性
  *   Java的构造器
  *     -默认提供一个无参的构造器，如果显式提供了带参的构造器，无参构造将不再提供，如果需要通过无参构造器
  *       创建对象，需要显式提供无参构造器
  *     -构造方法名称和类名保持一致
  *     -构造方法没有返回值类型，void都没有
  *     -构造方法支持重载
  *   Scala的构造器
  *     -Scala的构造器分为主构造器和辅助构造器
  *     -主构造器
  *       *在声明类的同时，也将主构造器声明了
  *       *在类名的后面，可以跟构造器的参数，如果无参，那么类后面的括号可以省略
  *     -辅助构造器
  *       *方法名必须叫this
  *       *没有返回值类型
  *       *在辅助构造器第一行代码位置，必须直接或者间接的调用主构造器  通过this调用
  *       *辅助构造器支持重载
  *
  */
object Scala08_TestConstructor {
  def main(args: Array[String]): Unit = {
    //val std = new Student08
    //val std = new Student08("zhangsan")
    val std = new Student08("zhangsan",20)

  }
}

class Student08{
  println("主构造器被执行了")

  var name:String = "zhangsan"
  var age:Int = 20

  //辅助构造器
  def this(name:String){
    this()
    println("辅助构造器1被执行了")
    this.name = name
  }

  def this(name:String,age:Int){
    this(name)
    println("辅助构造器2被执行了")
    this.age = age
  }



  //以下方法不是构造方法
  def Student08(){
    println("构造方法被执行了")
  }
}
