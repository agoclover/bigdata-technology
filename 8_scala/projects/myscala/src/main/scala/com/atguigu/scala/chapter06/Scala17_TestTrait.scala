package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 特质的声明以及混入
  */
object Scala17_TestTrait {
  def main(args: Array[String]): Unit = {
    //动态混入
    val ss: Teacher17 with TraitBuyBJP = new Teacher17 with TraitBuyBJP {
      override def buy(): Unit = {
        "hgrsw"
      }
    }
    ss

  }
}

trait TraitA{
  def run():Unit
}

trait TraitB{
  def speak():Unit
}

abstract class MySuperClass{
  def study():Unit
}

trait TraitBuyBJP{
  def buy():Unit
}

//class MySubClass extends  TraitA with TraitB{
class Teacher17 extends MySuperClass with TraitA with TraitB with Serializable {
  override def study(): Unit = {
    println("study")
  }

  override def run(): Unit = {
    println("run")
  }

  override def speak(): Unit = {
    println("speak")
  }
}