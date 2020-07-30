package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/10
  * Desc:
  * 为什么序列化
  *   -因为Spark程序初始化操作发生在Driver端，具体算子的执行是在Executor端执行的
  *   如果在Executor执行的时候，要访问Driver端初始化的数据，那么就涉及跨进程或者跨节点之间的通信，
  *   所以要求传递的数据必须是可序列化
  *如何确定是否序列化
  *   -在执行RDD相关算子之前，有一段这样的代码val cleanF = sc.clean(f)，判断是否进行了闭包检测
  *   -之所以叫闭包检测，因为算子也是一个函数，算子函数内部访问了外部函数的局部变量，形成了闭包
  */
object Spark06_TestSerializable {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val ydcx: User = new User
    ydcx.name = "ydcx"

    val tnl: User = new User
    tnl.name = "tnl"

    val userRDD: RDD[User] = sc.makeRDD(List(ydcx,tnl))

    userRDD.foreach(println(_))

    // 关闭连接
    sc.stop()
  }
}
class User {
  var name:String = _

  override def toString = s"User($name)"
}
