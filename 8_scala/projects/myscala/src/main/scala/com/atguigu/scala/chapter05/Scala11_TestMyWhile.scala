package com.atguigu.scala.chapter05

/**
  * Author: Felix
  * Date: 2020/6/30
  * Desc:   自定通过函数实现while循环的效果
  */
object Scala11_TestMyWhile {
  def main(args: Array[String]): Unit = {
    var a:Int = 1
    /*while (a <= 5){
      println(a)
      a += 1
    }*/
   mywhile(a<=5,{
     println(a)
     a += 1
   })
  }

  //不用柯里化、也不用函数的嵌套  就是最普通的函数实现mywhile
  def mywhile(condition: =>Boolean,op: =>Unit): Unit ={
    if(condition){
      op
      mywhile(condition,op)
    }
  }
  /*
  //不用柯里化 ，使用函数的嵌套实现mywhile
  def mywhile(condition: =>Boolean): (=>Unit)=>Unit ={
    def ff(op: =>Unit): Unit ={
      if(condition){
        op
        mywhile(condition)(op)
      }
    }
    ff _
  }
  */

  //自定义函数，实现while循环效果
  /*
  def mywhile(condition: =>Boolean)(op: =>Unit): Unit ={
    if(condition){
      op
      mywhile(condition)(op)
    }
  }
  */


}
