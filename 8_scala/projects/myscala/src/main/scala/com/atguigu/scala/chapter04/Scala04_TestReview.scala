package com.atguigu.scala.chapter04

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 
  */
object Scala04_TestReview {
  def main(args: Array[String]): Unit = {
    for(i<-1 to 18 by 2;j=(18-i)/2){
      println(" "*j + "*"*i)
    }
  }
}
