package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc:  抽象
  *   -模糊不具体
  *   -抽象属性
  *     如果一个属性只有声明，没有具体赋值
  *   -抽象方法
  *     如果一个方法只有声明没有实现
  *   -如果一个类中存在抽象属性或者抽象方法，那么这个类一定是抽象类
  *   -如果一个类是抽象类，那么这个类中不一定包含抽象属性和抽象方法
  *   -如果一个类是抽象类，抽象的属性或者方法一般交给子类实现；如果子类也不能对抽象内容进行实现，那么子类
  *     也应该定义为抽象类
  */
object Scala11_TestAbstract {

}

abstract class YG{
  def move():Unit
}

class YGSon extends YG{
  override def move(): Unit = {
    println("~~~~铁锹上~~~~")
  }
}
abstract class YGDau extends YG{
}

class YGWS extends YGDau{
  override def move(): Unit = {
    println("蓝翔~~~")
  }
}


