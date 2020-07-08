package com.atguigu.scala.chapter06

/**
  * Author: Felix
  * Date: 2020/7/1
  * Desc：通过特质叠加  解决特质冲突的钻石问题
  */
object Scala19_TestTrait {

}

trait Operation {
  def describe(): String = {
    "插入数据"
  }
}

trait OperationDB extends Operation {
  override def describe(): String = {
    "向数据库中" + super.describe()
  }
}

trait OperationHDFS extends Operation {
  override def describe(): String = {
    "向HDFS中" + super.describe()
  }
}

class MyClass19 extends  OperationHDFS with OperationDB {
  override def describe(): String = {
    "我的操作是 " + super.describe()
  }
}

object TestTrait {
  def main(args: Array[String]): Unit = {
    println(new MyClass19().describe())
  }
}

