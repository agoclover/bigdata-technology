package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Author: Felix
  * Date: 2020/7/13
  * Desc: 热门品类Top10中活跃用户Top10
  *      -活跃用户 通过点击数进行判断
  */
object Spark05_Req2 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //读取外部文件，创建RDD
    val lineRDD: RDD[String] = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")

    //对读取到的一行数据进行封装，封装为UserVisitAction对象，放到RDD中
    val userVisitActionRDD: RDD[UserVisitAction] = lineRDD.map(
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    )

    //最终:CategoryCountInfo(品类id,clickCount,orderCount,payCount)
    //(品类A,1,0,0)
    //(品类A,0,1,0)
    //(品类A,0,0,1)
    //(品类A,1,1,1)
    //对用户访问行为进行分析，判断是哪种行为日志，转换为对应的CategoryCountInfo对象
    val infoRDD: RDD[CategoryCountInfo] = userVisitActionRDD.flatMap(
      userAction => {
        //判断是否为点击操作
        if (userAction.click_category_id != -1) {
          val clickList = new ListBuffer[CategoryCountInfo]()
          clickList.append(CategoryCountInfo(userAction.click_category_id.toString, 1, 0, 0))
          clickList
        } else if (userAction.order_category_ids != "null") {
          //判断是否为下单操作
          val orderList = new ListBuffer[CategoryCountInfo]()
          val ids: Array[String] = userAction.order_category_ids.split(",")
          for (id <- ids) {
            orderList.append(CategoryCountInfo(id, 0, 1, 0))
          }
          orderList
        } else if (userAction.pay_category_ids != "null") {
          //判断是否为支付操作
          val payList = new ListBuffer[CategoryCountInfo]()
          val ids: Array[String] = userAction.pay_category_ids.split(",")
          for (id <- ids) {
            payList.append(CategoryCountInfo(id, 0, 0, 1))
          }
          payList
        } else {
          Nil
        }
      }
    )

    //分组：将相同的品类放到一组
    val groupRDD: RDD[(String, Iterable[CategoryCountInfo])] = infoRDD.groupBy(_.categoryId)

    //对分组之后的数据进行聚合求和
    val reduceRDD: RDD[(String, CategoryCountInfo)] = groupRDD.mapValues(
      datas => {
        datas.reduce((info1, info2) => {
          info1.clickCount = info1.clickCount + info2.clickCount
          info1.orderCount = info1.orderCount + info2.orderCount
          info1.payCount = info1.payCount + info2.payCount
          info1
        })
      }
    )

    //对RDD结构进行转换
    val mapRDD: RDD[CategoryCountInfo] = reduceRDD.map(_._2)

    //降序排序(clickCount,orderCount,payCount)  取前10
    val res: Array[CategoryCountInfo] = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false).take(10)

    //--------------需求二--------------------
    //	通过需求1，获取TopN热门品类的id
    val categoryIDs: Array[String] = res.map(_.categoryId)

    //	将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      userAction => {
        //坑1：   热门品类id是字符串数组，是否包含 获取的userAction.click_category_id是long类型，需要转换为字符串
        userAction.click_category_id != -1 && categoryIDs.contains(userAction.click_category_id.toString)
      }
    )

    //	对session的点击数进行转换 (category-session,1)
    //坑2：session-id中的值有-，所以我们这里的连接符号避免-
    val mapRDD11: RDD[(String, Int)] = filterRDD.map(
      userAction => (userAction.click_category_id + "_" + userAction.session_id, 1)
    )

    //	对session的点击数进行统计 (category-session,sum)
    val reduceRDD11: RDD[(String, Int)] = mapRDD11.reduceByKey(_+_)

    //	将统计聚合的结果进行转换  (category,(session,sum))
    val mapRDD22: RDD[(String, (String, Int))] = reduceRDD11.map {
      case (cateAndSession, count) => {
        (cateAndSession.split("_")(0), (cateAndSession.split("_")(1), count))
      }
    }

    //	将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val groupRDD22: RDD[(String, Iterable[(String, Int)])] = mapRDD22.groupByKey()

    //	对分组后的数据降序 取前10
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD22.mapValues(
      datas => datas.toList.sortBy(-_._2).take(10)
    )
    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
