package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-groupByKey  通过key对RDD中的元素进行分组
  */
object Spark13_Transformation_groupByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    //val newRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    //按照key对RDD中元素进行分组
    val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    //扩展：分组之后，对当前key对应的value集合进行求和
    val resRDD: RDD[(String, Int)] = newRDD.map {
      case (key, values) => {
        (key, values.sum)
      }
    }

    resRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}