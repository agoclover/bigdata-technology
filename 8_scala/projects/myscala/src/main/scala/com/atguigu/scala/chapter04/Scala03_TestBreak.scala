package com.atguigu.scala.chapter04

//相当于Java中的静态导入
import scala.util.control.Breaks._

/**
  * Author: Felix
  * Date: 2020/6/29
  * Desc: 循环中断
  *   在java语言中，跳出循环有两种方式
  *     -continue
  *         跳出本次循环，继续下一次循环
  *     -break
  *         跳出整个循环
  *
  *   在Scala语言中，没有break和continue关键
  *     -通过在循环体中加判断或者循环守卫实现continue效果
  *     -
  *
  */
object Scala03_TestBreak {
  def main(args: Array[String]): Unit = {

    //1.通过抛出异常的方式模拟break关键字，跳出整个循环
    /*try{
      for (elem <- 1 to 10) {
        println(elem)
        if(elem==5){
          //抛出异常
          throw new NullPointerException
        }
      }
    }catch{
      case e:Exception=>
    }

    //2.通过Scala中Breaks类中提供的方法，模拟break关键字
    Breaks.breakable{
      for (elem <- 1 to 10) {
        println(elem)
        if(elem==5) Breaks.break()
      }
    }
    */

    //简化
    breakable{
      for (elem <- 1 to 10) {
        println(elem)
        if(elem==5) break
      }
    }

    println("程序的其它代码")
  }
}
