package com.atguigu.scala.chapter04
import scala.collection.immutable

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc: 循环
  */
object Scala02_TestFor {
  def main(args: Array[String]): Unit = {
    //Scala中的for循环类似于java中的增强for循环
    //范围数据循环
    //for(a:Int <- 1 to 5){
    //  println(a)
    //}

    //for(a:Int <- 1 until 5){
    //  println(a)
    //}

    //for(a <- 1 to 5){
    //  if(a!=2){
    //    println(a)
    //  }
    //}

    //for(a <- 1 to 5; if a!=2 ){
    //  println(a)
    //}

    //for( a <- 1 to 10 by 2){
    //  println(a)
    //}

    //for( a <-5 to 1 by -1){
    //  println(a)
    //}

    //循环嵌套
    //for(i<-1 to 5;j<-1 to 3){
    //  println("i="+ i + ",j=" + j)
    //}

    //循环变量
    //for(i <- 1 to 3 ; j = 4 - i) {
    //  println("i=" + i + " j=" + j)
    //}

    //for {
    //  i <- 1 to 3
    //  j = 4 - i
    //} {
    //  println("i=" + i + " j=" + j)
    //}

    //val ints = for(a <- 1 to 10) yield {
    //  a*2
    //}
    //println(ints)

    for(i <- 1 to 5 reverse){
      println(i)
    }

  }
}
