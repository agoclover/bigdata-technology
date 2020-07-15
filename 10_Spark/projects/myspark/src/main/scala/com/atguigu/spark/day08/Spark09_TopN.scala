package com.atguigu.spark.day08

import java.text.DecimalFormat

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: TopN案例
  */
object Spark09_TopN {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .enableHiveSupport()
      .getOrCreate()

    //创建函数对象
    val cityRemark = new MyRemarkUDAF
    //注册函数
    spark.udf.register("cityRemark",functions.udaf(cityRemark))

    //--1.查询用户行为表，过滤出点击行为，和城市以及商品表进行连接查询
    spark.sql(
      """
        |select
        |    c.*,
        |    p.product_name
        |from
        |    user_visit_action a
        |left join
        |    city_info c
        |on
        |    a.city_id = c.city_id
        |left join
        |    product_info p
        |on
        |    a.click_product_id = p.product_id
        |where
        |    a.click_product_id != -1
      """.stripMargin).createOrReplaceTempView("t1")

    //--2.按照地区和商品名称分组，统计出每个商品在每个地区的总点击次数
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.product_name,
        |    count(*) as product_click_count,
        |    cityRemark(t1.city_name)
        |from
        |    t1
        |group by t1.area,t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")

    //--3.使用排序窗口函数，对每个地区内，按照点击次数降序排列
    spark.sql(
      """
        |select
        |    t2.*,
        |    ROW_NUMBER() over(partition by t2.area order by t2.product_click_count desc) cn
        |from
        |    t2
      """.stripMargin).createOrReplaceTempView("t3")

    //--4.对排序的结果取前3名
    spark.sql(
      """
        |select
        |    t3.*
        |from
        |    t3
        |where cn <= 3
      """.stripMargin).show(false)
  }
}

//缓存类型      城市点击数  mutable.Map("北京"->1,"天津"->1), 总点击数 Long
case class RemarkBuffer(var cityClickCountMap:mutable.Map[String,Long],var totalCount:Long)

//输出结果的样例类
case class CityRemark(cityName: String, cityRatio: Double) {
  val formatter = new DecimalFormat("0.00%")
  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}


class MyRemarkUDAF extends Aggregator[String,RemarkBuffer,String]{

  //设置缓存初始值
  override def zero: RemarkBuffer = RemarkBuffer(mutable.Map[String,Long](),0L)

  //对传入的城市进行聚合
  override def reduce(buf: RemarkBuffer, cityName: String): RemarkBuffer = {
    buf.cityClickCountMap(cityName) = buf.cityClickCountMap.getOrElse(cityName,0L) + 1L
    buf.totalCount += 1L
    buf
  }

  //多个缓存区合并
  override def merge(b1: RemarkBuffer, b2: RemarkBuffer): RemarkBuffer = {
    var map1 = b1.cityClickCountMap
    var map2 = b2.cityClickCountMap
    b1.cityClickCountMap = map1.foldLeft(map2)((mm,kv)=>{
      val k: String = kv._1
      val v: Long = kv._2
      mm(k) = mm.getOrElse(k,0L) + v
      mm
    })
    b1.totalCount += b2.totalCount
    b1
  }

  //输出     "北京21.2%，天津13.2%，其他65.6%"
  override def finish(buff: RemarkBuffer): String = {
    val cityMap: mutable.Map[String, Long] = buff.cityClickCountMap
    val totalCount: Long = buff.totalCount

    var remarkList: List[CityRemark] = cityMap.toList.sortBy(-_._2).take(2).map {
      case (city, cityCount) => {
        CityRemark(city, cityCount.toDouble / totalCount)
      }
    }
    //如果城市的数量大于2
    if(cityMap.size > 2){
      remarkList = remarkList :+ CityRemark("其他", remarkList.foldLeft(1D)(_ - _.cityRatio))
    }
    remarkList.mkString(",")
  }

  //指定编解码方式，固定写法
  override def bufferEncoder: Encoder[RemarkBuffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}

