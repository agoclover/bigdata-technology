package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 对RDD中的元素进行映射--以分区为单位  并带分区号
  */
object Spark08_Transformation_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过集合创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9),3)
    println("分区数："  + rdd.partitions.size)

   /*
   rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println("分区号："+index + ":::" +datas.mkString(","))
        datas
      }
    }.collect()
    */

    println("~~~~~~~~~~~~~~~~~~~~~~~")

    /*//使每个元素跟所在分区号形成一个元组，组成一个新的RDD
    val newRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
      case (index, datas) => {
        datas.map((index, _))
      }
    }
    newRDD.collect().foreach(println)*/

    //扩展功能，对二号分区中的数据*2，其它分区中数据不变
    val newRDD: RDD[Int] = rdd.mapPartitionsWithIndex {
      case (index, datas) => {
        index match {
          case 2 => datas.map(_ * 2)
          case _=> datas
        }
      }
    }
    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
