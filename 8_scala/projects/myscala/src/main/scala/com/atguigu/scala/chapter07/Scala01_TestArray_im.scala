package com.atguigu.scala.chapter07

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc:  不可变数组
  */
object Scala01_TestArray_im {
  def main(args: Array[String]): Unit = {
    //创建数组对象  方式1
    //val arr: Array[Int] = new Array[Int](5)
    val arr: Array[Int] = Array(1,2,3)

    //访问数组中元素   通过数组下标访问数组元素  数组名(下标)
    //println(arr(1))

    /*
    //遍历数组中的元素方式1   普通for循环
    for( i <- 0 until arr.length){
      println(arr(i))
    }
    //遍历数组中的元素方式2   普通for循环(简化)
    for(elem <- arr){
      println(elem)
    }
    //遍历数组中的元素方式3 迭代器
    val it: Iterator[Int] = arr.iterator
    while(it.hasNext){
      println(it.next())
    }
    //遍历数组中的元素方式4 迭代器(简化版)
    for (elem <- arr.iterator) {
      println(elem)
    }
    //遍历数组中的元素方式5 foreach
    //arr.foreach((elem:Int)=>{
    //  println(elem)
    //})
    arr.foreach(println)
    */

    //遍历数组中的元素方式6 mkString   用指定的字符串连接数组中的元素，形成一个新的字符串
    //println(arr.mkString("-"))

    arr.foreach(println)

    println(arr.hashCode())
    println("--------------------------")
    //添加元素
    //val newArr: Array[Int] = arr.+:(10)
    //val newArr: Array[Int] = arr.:+(10)

    //arr.foreach(println)
    //newArr.foreach(println)
    //println(newArr.hashCode())
  }
}
