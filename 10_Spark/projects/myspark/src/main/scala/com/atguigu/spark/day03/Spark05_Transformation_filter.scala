package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-filter   对RDD中的元素进行过滤
  */
object Spark05_Transformation_filter {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //需求：分区最大值求和
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),3)
    rdd.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "---->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("~~~~~~~~~~~~~~~")
    //将RDD中的偶数过滤出来

    val filterRDD: RDD[Int] = rdd.filter(_%2==0)

    filterRDD.mapPartitionsWithIndex{
      (index,datas)=>{
        println(index + "---->" + datas.mkString(","))
        datas
      }
    }.collect()

    // 关闭连接
    sc.stop()
  }
}
