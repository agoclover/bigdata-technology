package com.atguigu.spark.day06.studentShow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top10_TLQ {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Top10")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")
    //得到搜索词不为null 的 数据 点击数等封装 在list
    val idRdd= rdd.map {
      line => {
        val strings = line.split("_")
        (strings(6), strings(8), strings(10))
      }
    }


    //id 点击数 下单数 支付数
    // strings:  list( id/null , Array(null/id,id) , Array(null/id,id)
    val clickRdd = idRdd.map {
      case (click,order,pay) => (click, 1)
    }.reduceByKey(_ + _)

    val orderRdd: RDD[(String, Int)] = idRdd.map {
      case (click,order,pay) => order
    }.filter(_ != null).flatMap {
      order => order.split(",").map((_, 1))
    }.reduceByKey(_ + _)

    val payRdd: RDD[(String, Int)] = idRdd.map {
      case (click,order,pay) => pay
    }.filter(_!= null).flatMap {
      case pay => pay.split(",").map((_, 1))
    }.reduceByKey(_ + _)


    //三个表join连接 然后排序取前十
    //（id,((click,order),pay))
    val finalRdd: Array[(String, Int, Int, Int)] = clickRdd.leftOuterJoin(orderRdd).leftOuterJoin(payRdd).map {
      case (id, count) => {
        val click = count._1._1
        val order = count._1._2.getOrElse(0)
        val pay = count._2.getOrElse(0)

        (id, click, order, pay)
      }
    }.sortBy(x => (-x._2, -x._3, -x._4)).take(10)


    finalRdd.foreach(println)
    sc.stop()
  }

}
