package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/13
  * Desc: 累加器   分布式共享只写变量
  */
object Spark01_Accumulator {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    //val resRDD: RDD[(String, Int)] = dataRDD.reduceByKey(_+_)
    //resRDD.collect().foreach(println)

    /*
    //定义一个变量接收出现次数和
    var sum:Int = 0

    dataRDD.foreach{
      case (_,count)=>{
        sum +=count
      }
    }
    println(sum)
    */

    //使用累加器 接收每一个Task计算结果
    //var sum:Int = 0
    //使用自带累加器
    //val sum: LongAccumulator = sc.longAccumulator
    //使用自定义累加器
    //创建累加器对象
    val myac = new MyLongAccumulator
    //注册累加器
    sc.register(myac)
    //使用累加器
    dataRDD.foreach{
      case (_,count)=>{
        //sum +=count
        myac.add(count)
        println(myac.value+"~~~~")
      }
    }
    //println(sum)
    println(myac.value)

    // 关闭连接
    sc.stop()
  }
}


class MyLongAccumulator extends AccumulatorV2[Int, Int] {
  //定义一个变量，获取累加结果
  private var sum:Int = 0

  //判断是否为初始值
  override def isZero: Boolean = sum==0

  //拷贝
  override def copy(): AccumulatorV2[Int, Int] = {
    val myac = new MyLongAccumulator
    myac.sum = this.sum
    myac
  }

  //恢复初始状态
  override def reset(): Unit = sum=0

  //累加
  override def add(v: Int): Unit = {
    sum += v
  }

  //合并Task数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    this.sum += other.value
  }

  //获取累加器的值
  override def value: Int = sum
}