package com.atguigu.scala.chapter08

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc:  扩展案例
  */
object Scala02_TestMatch {
  def main(args: Array[String]): Unit = {
    //val list: List[(String, Int)] = List(("a",1),("b",2),("c",3))
    //打印元组中第一个元素
    //for (tuple <- list) {
    //  println(tuple._1)
    //}
    //for ((word,count) <- list) {
    //for ((word,_) <- list) {
    //for (("a",count) <- list) {
    //  println(count)
    //}

    ////特殊的模式匹配2 给元组元素命名
    //val (id,name,age): (Int, String, Int) = (1000,"zhangsan",20)
    //println(id)
    //println(name)
    //println(age)

    /*//特殊的模式匹配3   遍历集合中的元组，给count * 2
    var list: List[(String, Int)] = List(("a", 1), ("b", 2), ("c", 3))
    //println(list.map(tuple => (tuple._1, tuple._2 * 2)))

    //在函数的参数中，如果匿名函数使用模式匹配case，要求函数参数的括号必须是{}
    //将函数的参数括号改为{}是有要求的，只当函数的参数列表中的参数是1个的时候，才可以
    val res: List[(String, Int)] = list.map {
      // 错误的写法  ((a:String,b:Int))=>{}
      case (word, count) => {
        (word, count * 2)
      }
    }
    println(res)*/

    var list1 = List(("a", ("a", 1)), ("b", ("b", 2)), ("c", ("c", 3)))

   /*
   val res: List[(String, (String, Int))] = list1.map(
      tuple => {
        (tuple._1, (tuple._2._1, tuple._2._2 * 2))
      }
    )
    println(res)
    */
    val res: List[(String, (String, Int))] = list1.map {
      case (key, (word, count)) => {
        (key, (word, count * 2))
      }
    }
    println(res)
  }
}
