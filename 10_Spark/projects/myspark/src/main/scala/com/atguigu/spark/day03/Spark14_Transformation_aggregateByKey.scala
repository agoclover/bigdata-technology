package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-aggragateByKey  根据key对分区内和分区间的数据进行聚合
  */
object Spark14_Transformation_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()
    //分区最大值求和
    //rdd.aggregateByKey(0)((a,b)=>math.max(a,b),(x,y)=>x+y)
    //val newRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)
    val newRDD: RDD[(String, Int)] = rdd.aggregateByKey(5)(math.max(_,_),_+_)
    newRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}