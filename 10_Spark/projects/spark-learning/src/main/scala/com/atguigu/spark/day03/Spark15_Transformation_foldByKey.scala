package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-foldByKey  根据key对分区内和分区间的数据进行聚合
  *     是aggregateByKey的简化，分区内和分区间计算规则相同
  */
object Spark15_Transformation_foldByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",1),("a",1),("b",1),("b",1),("b",1),("b",1),("a",1)),2)

    //val resRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
    val resRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    resRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}