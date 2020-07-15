package com.atguigu.spark.day08

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/15
  * Desc: 通过自定义UDAF(弱类型)方式实现求平均年龄
  *       一般应用SQL风格
  */
object Spark03_AvgUDAF {
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
    val myAvg = new MyAvgUDAF
    //注册函数对象
    spark.udf.register("myAvg",myAvg)

    //使用函数进行查询
    spark.sql("select myAvg(age) from people").show
  }
}
//select myavg(age) from user
class MyAvgUDAF extends UserDefinedAggregateFunction{
  //输入数据类型
  override def inputSchema: StructType = StructType(List(StructField("age",IntegerType)))

  //缓存数据类型
  override def bufferSchema: StructType = {
    StructType(List(StructField("ageSum",LongType),StructField("count",LongType)))
  }

  //输出数据类型
  override def dataType: DataType = DoubleType

  //精准性校验
  override def deterministic: Boolean = true

  //设置缓存初始状态
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新缓存状态
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val age: Int = input.getInt(0)
    buffer(0) = buffer.getLong(0) + age
    buffer(1) = buffer.getLong(1) + 1L
  }

  //合并缓存数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //返回输出结果
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
