package com.atguigu.scala.chapter11

/**
  * Author: Felix
  * Date: 2020/7/4
  * Desc: Scala的泛型
  */
class Parent{}
class Child extends Parent{}
class Sub extends Child{}

//泛型模板
//不可变
//class MyList[T]{}
//协变
//class MyList[+T]{}
//逆变
//class MyList[-T]{}


object Scala01_TestGeneric {
  def main(args: Array[String]): Unit = {
    //var ml:MyList[Child] = new MyList[Child]
    //var m2:MyList[Child] = new MyList[Parent]
    //var m3:MyList[Child] = new MyList[Sub]
    test(classOf[Child])
    //test(classOf[Parent])
    test(classOf[Sub])
  }

  //泛型通配符
  //下界
  /*def test[A >: Child ](clz:Class[A]): Unit ={
    println(clz.getName)
  }*/
  //上界
  def test[A <: Child ](clz:Class[A]): Unit ={
    println(clz.getName)
  }
}
