package com.atguigu.spark.day08

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: 通过累加器实现求平均年龄
  */
object Spark02_AvgAccumulator {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    //创建累加器对象
    val myac = new MyAccumulator11
    //注册累加器
    sc.register(myac)
    //使用累加器
    rdd.foreach{
      case (name,age)=>{
        myac.add(age)
      }
    }
    println(myac.value)
    // 关闭连接
    sc.stop()
  }
}
class MyAccumulator11 extends AccumulatorV2[Int,Double]{
  private var sum:Int = 0
  private var count:Int = 0

  override def isZero: Boolean = {
    sum==0 && count==0
  }

  override def copy(): AccumulatorV2[Int, Double] = {
    val myac = new MyAccumulator11
    myac.sum = this.sum
    myac.count = this.count
    myac
  }

  override def reset(): Unit = {
    sum = 0
    count = 0
  }

  override def add(age: Int): Unit = {
    sum += age
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    /*if(other.isInstanceOf[MyAccumulator11]){
      val oo: MyAccumulator11 = other.asInstanceOf[MyAccumulator11]
      sum += oo.sum
      count += oo.count
    }*/
    other match {
      case myac:MyAccumulator11=>{
        sum += myac.sum
        count += myac.count
      }
      case _=>
    }
  }

  override def value: Double = sum.toDouble/count
}
