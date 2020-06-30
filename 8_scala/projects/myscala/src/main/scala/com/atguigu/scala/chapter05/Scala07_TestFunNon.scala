package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc: 匿名函数  --- 两个整数运算
  */
object Scala07_TestFunNon {
  def main(args: Array[String]): Unit = {
    def calculator(a:Int,b:Int,op:(Int,Int)=>Int): Int ={
      op(a,b)
    }

    //calculator(10,20,_+_)
    //println(calculator(10,20,(x:Int,y:Int)=>{x + y}))
    calculator(10,20,_*_)
  }
}
