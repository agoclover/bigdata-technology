package com.atguigu.spark.day08

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: 通过自定义UDAF(新版弱类型)实现求平均年龄
  */
object Spark05_AvgUDAF_new {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()
    import spark.implicits._
    //创建DataFrame
    val df: DataFrame = spark.read.json("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\test.json")

    //创建临时视图
    df.createOrReplaceTempView("people")

    //创建函数对象
    val myAvg = new MyAvgUDAF2
    //注册函数对象
    spark.udf.register("myAvg",functions.udaf(myAvg))

    //使用函数进行查询
    spark.sql("select myAvg(age) from people").show
  }
}
//定义一个类，继承Aggregator  select myavg(age) from people
class MyAvgUDAF2 extends Aggregator[Int,AgeBuffer,Double]{
  //设置初始状态
  override def zero: AgeBuffer = AgeBuffer(0L,0L)

  //累加
  override def reduce(buf: AgeBuffer, age: Int): AgeBuffer = {
    buf.sum += age
    buf.count += 1
    buf
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(buf: AgeBuffer): Double = {
    buf.sum.toDouble/buf.count
  }

  override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
