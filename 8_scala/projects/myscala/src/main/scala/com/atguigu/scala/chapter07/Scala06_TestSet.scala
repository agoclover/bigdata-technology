package com.atguigu.scala.chapter07

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/3
  * Desc: Set集合
  */
object Scala06_TestSet {
  def main(args: Array[String]): Unit = {
    //Set集合特点：无序，不能重复
    /*
    //不可变Set集合
    val set: Set[Int] = Set(6,7,1,2,3,4,4,3,2,1,5,8)
    val newSet: Set[Int] = set + (10)
    newSet.foreach(println)
    */
    val set: mutable.Set[Int] = mutable.Set(1,2,3)
    set.add(10)
    //set.update(20,true)
    set.remove(10)
    println(set)
  }
}
