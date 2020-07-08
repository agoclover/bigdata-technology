package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-改变分区
  *   -coalesce
  *      默认不shuffle
  *      一般用于缩减分区
  *   -repartition
  *     底层调用的是coalesce，只不过默认执行shuffle操作
  *     一般用于扩大分区
  */
object Spark08_Transformation_changePartition {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),3)
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("~~~~~~~~~~~~")
    //缩减分区  调用coalesce算子，默认没有shuffle
    //val newRDD: RDD[Int] = rdd.coalesce(2)

    //扩大分区  调用coalesce算子，因为默认没有shuffle，
    // 所以扩大分区没有意义，所以默认使用coalesce不能直接扩大分区
    //val newRDD: RDD[Int] = rdd.coalesce(4)

    //要想通过coalesce扩大分区，可以通过参数指定shuffle
    //val newRDD: RDD[Int] = rdd.coalesce(4,true)

    //实际在扩大分区的时候，使用repartition算子
    //val newRDD: RDD[Int] = rdd.repartition(2)
    val newRDD: RDD[Int] = rdd.repartition(4)

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
