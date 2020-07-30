package com.atguigu.myspark.day04

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/10 4:18 下午
 */
object S07_ClosureCheck {
  def main(args: Array[String]): Unit = {
    val check = new MyClosureCheck
    check.outerF()("par")

    val ss = new SomethingNotSerializable
    ss.someMethod()
  }
}

class SomethingNotSerializable {
  def someValue = 1

  def scope(name: String)(body: => Unit) = body

  def someMethod(): Unit = scope("one") {
    def x = someValue

    def y = 2

    scope("two") {
      println(y + 1)
    }
  }
}
class MyClosureCheck extends Serializable {
  val ouV = 1
  def outerF()={
    val inV = 2
    def innerF(str:String): Unit ={
      println(str + " " +  inV + " " + ouV)
    }
    innerF _
  }
}