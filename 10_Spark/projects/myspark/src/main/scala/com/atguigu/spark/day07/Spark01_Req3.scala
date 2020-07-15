package com.atguigu.spark.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Author: Felix
  * Date: 2020/7/13
  * Desc: 计算页面单跳转换率
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

    //---------------需求三实现--------------
    //1.计算分母
    //1.1 定义一个map集合 存放页面的总访问次数
    val fmMap: collection.Map[Long, Int] = userVisitActionRDD.map(userAction=>(userAction.page_id,1)).reduceByKey(_+_).collectAsMap()

    //2.计算分子
    //2.1按照sessionId对用户访问行为RDD数据进行分组
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)

    val mapValueRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        //2.2对分组之后的数据进行排序
        val sortList: List[UserVisitAction] = datas.toList.sortBy(_.action_time)
        //2.3对排序后的数据结构进行转换，只保留页面的id
        val userVisitList: List[Long] = sortList.map(_.page_id)
        //2.4通过拉链 形成页面单跳效果
        val pageFlowList: List[(Long, Long)] = userVisitList.zip(userVisitList.tail)
        //2.5对拉链之后的集合机构进行转换
        pageFlowList.map {
          case (pageA, pageB) => {
            (pageA + "_" + pageB, 1)
          }
        }
      }
    )
    //2.6 对上面RDD的结构进行转换，只保留页面跳转以及计数 (pageA_pageB,1)
    val pageFlowRDD: RDD[(String, Int)] = mapValueRDD.map(_._2).flatMap(list=>list)

    //2.7 对页面跳转情况进行汇总 (pageA_pageB,100)
    val reduceRDD: RDD[(String, Int)] = pageFlowRDD.reduceByKey(_+_)

    //3.计算页面单跳转换率
    reduceRDD.foreach{
      case (pageAToPageB,fz)=>{
        //3.1获取pageA页面的id
        val pageAId: String = pageAToPageB.split("_")(0)
        //3.2根据pageA页面id到fmMap集合中获取pageA页面的总访问量
        val fm: Int = fmMap.getOrElse(pageAId.toLong,1)
        println(pageAToPageB + "------>" + fz.toDouble / fm)
      }
    }

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
