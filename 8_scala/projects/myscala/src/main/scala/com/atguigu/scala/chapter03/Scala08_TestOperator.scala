package com.atguigu.scala.chapter03

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc:  运算符
  */
object Scala08_TestOperator {
  def main(args: Array[String]): Unit = {
    /*
    //=============算术运算符  ==============
    //val res: Int = 10/3
    //val res: Double = 10/3
    val res: Double = 10.0/3
    //res.formatted("%.2f")
    println(res.formatted("%.2f"))

    //=============比较运算符  ==============
    var s1: String = new String("abc")
    var s2: String = new String("abc")

    //在Scala语言中，equals也是比较String的内容是否相等
    //println(s1.equals(s2))
    //在Scala语言中，==和equals效果相同，都是比较String的内容
    //println(s1 == s2)

    //在Scala语言中，比较地址使用eq方法
    println(s1.eq(s2))
    */
    //println(isNotEmpty(null))
    //var b:Int = 10
    //b += 1
    //println(b)

    //1）当调用对象的方法时，点.可以省略
    //var a:Int = 1.+(1)
    //var a:Int = 1 +(1)
    //2）如果函数参数只有一个，或者没有参数，()可以省略
    //var a:Int = 1 + 1

    //println(123.toString())
    //println(123 toString())
    //println(123 toString)


  }

  //判断字符串是否不为空
  def isNotEmpty(s:String): Boolean ={
    return s!=null && !"".equals(s.trim)
  }
}
