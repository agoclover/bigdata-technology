package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-distinct  去除RDD中重复元素
  */
object Spark07_Transformation_distinct {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,4,3,2,6),3)
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("~~~~~~~~~~~~~~~~")
    //去除RDD中重复元素   默认产生的RDD的分区和原RDD分区个数相同
    //val newRDD: RDD[Int] = rdd.distinct()
    //去重之后，可以指定新的RDD的分区数
    val newRDD: RDD[Int] = rdd.distinct(2)
    newRDD.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    // 关闭连接
    sc.stop()
  }
}
