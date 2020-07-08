package com.atguigu.scala.chapter07

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc: 
  */
object Scala17_TestQueue {
  def main(args: Array[String]): Unit = {
    /*
    val que = new mutable.Queue[String]()
    que.enqueue("a", "b", "c")
    println(que.dequeue())
    println(que.dequeue())
    println(que.dequeue())
*/
    /*
    val que = new mutable.Stack[String]()
    que.push("a", "b", "c")
    println(que.pop())
    println(que.pop())
    println(que.pop())
    */

    //val result1 = (0 to 100).map{case _ => Thread.currentThread.getName}
    val result2 = (0 to 100).par.map{case _ => Thread.currentThread.getName}

    //println(result1)
    println(result2)

  }
}
