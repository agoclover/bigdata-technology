package com.atguigu.scala.chapter08

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc:  默认匹配基本语法
  *   需求：实现两个整数的简单四则运算
  */
object Scala01_TestMatch {
  def main(args: Array[String]): Unit = {
   /* var a:Int = 10
    var b:Int = 20
    var op:Char = 'd'
    //模式匹配有返回值，返回的是分支最后一行代码
    //break关键字不需要加
    val res = op match {
      case '+' => a + b
      case '-' => a - b
      case '*' => a * b
      case '/' => a / b
      case _ => "运算符不合法"
    }
    println(res)

    //使用模式匹配，获取一个整数的绝对值
    def abs(n:Int): Int ={
      n match {
        case i:Int if i>0 => i
        case i:Int if i<0 => -i
        case _=>0
      }
    }
    println(abs(-10))

    //模式匹配类型 - 匹配常量
    def describe(x: Any) = x match {
      case 5 => "Int five"
      case "hello" => "String hello"
      case true => "Boolean true"
      case '+' => "Char +"
    }
   //模式匹配类型 - 匹配类型
   def describe(x: Any) = x match {
     case i: Int => "Int"
     case s: String => "String hello"
     case m: List[String] => "List"
     case c: Array[Int] => "Array[Int]"
     case someThing => "something else " + someThing
   }
    //List集合支持泛型擦除
    //Array不支持泛型擦除
    println(describe(Array("a","b")))

    //模式匹配类型 - 匹配数组
    for (arr <- Array(
              Array(0),
              Array(1, 0),
              Array(0, 1, 0),
              Array(1, 1, 0),
              Array(1, 1, 0, 1),
              Array("hello", 90))) { // 对一个数组集合进行遍历
      val result = arr match {
        case Array(0) => "0" //匹配Array(0) 这个数组
        case Array(x, y) => x + "," + y //匹配有两个元素的数组，然后将将元素值赋给对应的x,
        case Array(0, _*) => "以0开头的数组" //匹配以0开头和数组
        case _ => "something else"
      }
      println("result = " + result)
    }


    //模式匹配类型 - 匹配List集合方式1
    //list是一个存放List集合的数组
    //请思考，如果要匹配 List(88) 这样的只含有一个元素的列表,并原值返回.应该怎么写
    for (list <- List(
              List(0),
              List(1, 0),
              List(0, 0, 0),
              List(1, 0, 0),
              List(88))) {
      val result = list match {
        case List(0) => "0" //匹配List(0)
        case List(x, y) => x + "," + y //匹配有两个元素的List
        case List(0, _*) => "0 ..."
        case List(a) => a
        case _ => "something else"
      }

      println(result)
    }

    //模式匹配类型 - 匹配List集合方式2
    val list: List[Int] = List(1, 2, 5, 6, 7)

    //first(1)::second(2)::rest(5,6,7)
    list match {
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }

    //模式匹配类型 - 匹配元组
    //对一个元组集合进行遍历
    for (tuple <- List(
            (0, 1),
            (1, 0),
            (1, 1),
            (1, 0, 2))) {
      val result = tuple match {
        case (0, _) => "0 ..." //是第一个元素是0的元组
        case (y, 0) => "" + y + "0" // 匹配后一个元素是0的对偶元组
        case (a, b) => "" + a + " " + b
        case _ => "something else" //默认

      }
      println(result)
    }
 */

  }
}
