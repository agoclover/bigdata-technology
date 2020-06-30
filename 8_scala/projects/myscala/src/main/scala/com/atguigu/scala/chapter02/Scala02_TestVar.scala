package com.atguigu.scala.chapter02

/**
  * Author: Felix
  * Date: 2020/6/28
  * Desc: 变量和常量
  *   -在Java语言中
  *     *变量
  *         数据类型  变量名 = 值
  *     *常量
  *         final 数据类型  变量名 = 值
  *   -在Scala语言中
  *     *变量
  *         var 变量名:数据类型 = 值
  *     *常量
  *         val 变量名:数据类型 = 值
  */
object Scala02_TestVar {
  def main(args: Array[String]): Unit = {
    //（0） 正确声明一个变量
    //var age:Int = 18
    //（1）声明变量时，类型可以省略，编译器自动推导，即类型推导
    //var age = 18
    //println(age)
    //（2）类型确定后，就不能修改，说明Scala是强数据类型语言。
    //var age:Int = 18
    //age = "zhangsan"
    //（3）变量声明时，必须要有初始值
    //var name:String=""
    //println(name)
    //（4）在声明/定义一个变量时，可以使用var或者val来修饰，var修饰的变量可改变，val修饰的变量不可改。
    //var name:String = "zhangsan"
    //val name:String = "zhangsan"
    //name = "lisi"
    //println(name)
    //（5）var修饰的对象引用可以改变，val修饰的对象则不可改变，但对象的状态（值）却是可以改变的。（比如：自定义对象、数组、集合等等）
//    var std: Student = new Student
    val std: Student = new Student
    std.name = "lisi"
    println(std.name)
    
  }
}
class Student{
  var name:String = "zhangsan"
}