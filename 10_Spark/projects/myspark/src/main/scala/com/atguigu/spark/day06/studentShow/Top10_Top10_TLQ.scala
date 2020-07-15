package com.atguigu.spark.day06.studentShow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object Top10_Top10_TLQ {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")
    //有效搜索
    val idRdd: RDD[(String, List[String])] = rdd.map {
      line => {
        val strings = line.split("_")
        (strings(5), List(strings(6), strings(8), strings(10)))
      }
    }.filter {
      case (str, list) => str != null
    }

    //有效点击
    val clickRdd = idRdd.map {
      case (str, strings) => {
        (strings.head, 1)
      }
    }.filter(_._1 != null).filter(_._1 != "-1").reduceByKey(_ + _)

    //有效订单
    val orderRdd: RDD[(String, Int)] = idRdd.map {
      case (str, strings) => (strings(1), 1)
    }.filter(_._1 != null).flatMap {
      case (str, count) => {
        str.split(",").map((_, 1))
      }
    }.reduceByKey(_ + _)

    //有效支付
    val payRdd: RDD[(String, Int)] = idRdd.map {
      case (str, strings) => (strings(2), 1)
    }.filter(_._1 != null).flatMap {
      case (str, count) => {
        str.split(",").map((_, 1))
      }
    }.reduceByKey(_ + _)

    //top10品类
    val topRdd: RDD[(String, Int, Int, Int)] = clickRdd.leftOuterJoin(orderRdd).leftOuterJoin(payRdd).map {
      case (id, count) => {
        val click = count._1._1
        val order = count._1._2.getOrElse(0)
        val pay = count._2.getOrElse(0)

        (id, click, order, pay)
      }
    }.sortBy(x => (-x._2, -x._3, -x._4))

    val top10: Array[(String, Int, Int, Int)] = topRdd.take(10)

    //得到品类id 和 sessionid、 通过top10ID 过滤
    val iDSessionIdRdd: RDD[(String, String)] = rdd.map {
      line => {
        val strings = line.split("_")
        (strings(6), strings(2))
      }
    }.filter {
      case (str, str1) =>
        val strings = new ListBuffer[String]
        for (elem <- top10) {
          strings.append(elem._1)
        }
        strings.contains(str)
    }

    //
    val top10SessionID10 = iDSessionIdRdd.map {
      case (str, str1) => (str + "," + str1, 1)
    }.reduceByKey(_ + _).map {
      case (str, sum) => {
        val strings = str.split(",")
        (strings(0), (strings(1), sum))
      }
    }.groupByKey().mapValues {
      datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }.take(10)
      }
    }


      top10SessionID10.foreach(println)

    sc.stop()
  }
}