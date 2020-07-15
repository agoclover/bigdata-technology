package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子
  *   glom      将一个RDD中分区中的每个元素放到一个数组中          个体->整体
  *   flatMap  对RDD中的元素进行扁平化处理，要求RDD中的元素是可迭代  整体->个体
  */
object Spark02_Transformation_glom {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    //通过集合创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)
    rdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "---->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("~~~~~~~~~~~~~~~~~~~~~~~")
    val newRDD: RDD[Array[Int]] = rdd.glom()
    newRDD.foreach(arr=>println(arr.mkString(",")))
    */
    //需求：分区最大值求和
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

    //将rdd每个分区中的元素合并到一个数组中
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = glomRDD.map(_.max)

    val res: Double = maxRDD.sum()

    println(res)
    // 关闭连接
    sc.stop()
  }
}
