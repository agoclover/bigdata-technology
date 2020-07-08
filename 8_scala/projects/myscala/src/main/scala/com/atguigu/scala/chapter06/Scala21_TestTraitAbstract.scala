package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc:
  *   特质和抽象类关系
  *     -抽象类中可以定义抽象属性、抽象方法、非抽象属性、非抽象方法；
            特质中可以定义抽象属性、抽象方法、非抽象属性、非抽象方法
  *     -抽象类不能被实例化；特质也不能被实例化
  *     -都可以被继承
  *     -优先选择特质，因为Scala是单继承，选择特质方便后续的扩展
  *     -一般在选择继承的时候，子类和父类应该满足  is-a
  *     -特质一般是对行为的抽象 ，定义规范
  *     -抽象类和特质都有构造方法
  *     -如果你需要构造函数参数，使用抽象类。因为抽象类可以定义带参数的构造函数，而特质不行（有无参构造）。
  *
  */
object Scala21_TestTraitAbstract {
  def main(args: Array[String]): Unit = {
    new MyC
  }
}

abstract class MyAbstract(name:String){
  println("抽象类构造方法")
}
trait MyTrait{
//trait MyTrait(name:String){
  println("特质构造方法")
}
class MyC extends MyAbstract("xx") with MyTrait{}
