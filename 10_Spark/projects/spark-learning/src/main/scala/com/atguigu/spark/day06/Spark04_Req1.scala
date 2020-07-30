package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Author: Felix
  * Date: 2020/7/13
  * Desc: 热门品类Top10
  */
object Spark04_Req1 {
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
    var res = mapRDD.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false).take(10)

    res.take(30).foreach(println)
    // 关闭连接
    sc.stop()
  }
}

//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo(categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数
