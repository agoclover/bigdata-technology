package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-sample  随机抽样
  */
object Spark06_Transformation_sample {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    //需求：分区最大值求和
    val rdd: RDD[Int] = sc.makeRDD(1 to 30,3)

    /*
    //对RDD中的元素进行随机抽样
    withReplacement: Boolean, 是否抽样放回
    fraction: Double,
        withReplacement=false，表示rdd中元素被抽取的概率  [0,1]
        withReplacement= true，表示期望RDD中元素被抽取的次数  >0
    seed: Long = Utils.random.nextLong    一般不需要设置
    */
    //val sampleRDD: RDD[Int] = rdd.sample(false,0)
    //val sampleRDD: RDD[Int] = rdd.sample(false,1)
    //val sampleRDD: RDD[Int] = rdd.sample(false,0.3)
    //val sampleRDD: RDD[Int] = rdd.sample(true,2)
    //sampleRDD.collect().foreach(println)
    
    //val ints: Array[Int] = rdd.takeSample(false,3)

    val lucyMan: Array[Int] = rdd.takeSample(false,1)
    lucyMan.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
