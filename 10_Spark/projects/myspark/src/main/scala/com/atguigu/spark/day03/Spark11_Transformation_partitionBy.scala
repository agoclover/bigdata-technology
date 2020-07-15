package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-partitionBy  使用指定的分区器按照key对RDD中的元素进行重新的分区
  */
object Spark11_Transformation_partitionBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(List((1,"aaa"),(2,"bbb"),(3,"cccc")),3)
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()
    println("~~~~~~~~~~~~~~~~~~~~~~~")
    //val newRDD: RDD[(Int, String)] = rdd.partitionBy(new HashPartitioner(2))
    val newRDD: RDD[(Int, String)] = rdd.partitionBy(new MyPartitioner(2))
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

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    1
    //8
    /*val phoneNum: String = key.asInstanceOf[String]
    if(phoneNum.startsWith("135")){
      0
    }else if(phoneNum.startsWith("136")){
      1
    }else{
      2
    }*/
  }
}