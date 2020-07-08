package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc: 访问权限
  *   Java中的访问权限
  *     private     当前类
  *     default     （默认）当前类、同包的其它类
  *     protected   当前类、同包的其它类、非同包的子类
  *     public      公开
  *   Scala访问权限
  *     -Scala 中属性和方法的默认访问权限为public，但Scala中无public关键字
  *     -private为私有权限，只在类的内部和伴生对象中可用
  *     -protected为受保护权限，Scala中受保护权限比Java中更严格，同类、子类可以访问，同包无法访问。
  *     -private[包名]增加包访问权限，包名下的其他类也可以使用
  */
object Scala06_TestPri {

}
class Person06{
  //私有
  private var stdNo:String = "1000"
  //受保护的
  protected var name:String = "atguigu"
  //公开的
  var sex:String = "女"
  //包权限
  private[chapter06] var age:Int = 100

  def m1(): Unit ={
    println(stdNo)
    println(name)
    println(sex)
    println(age)
  }
}
