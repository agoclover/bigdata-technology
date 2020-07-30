package com.atguigu.spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author: Felix
  * Date: 2020/7/14
  * Desc: SparkSQL入门案例
  */
object Spark02_SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQLDemo")

    //创建SparkSQL程序执行的入口  SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //注意：spark不是包名，是SparkSession的别名
    import spark.implicits._
    //通过读取json文件，创建DF
    //val df: DataFrame = spark.read.json("D:\\dev\\workspace\\bigdata-0317\\spark-0317\\input\\test.json")

    //df.show()
    /*
    //---------通过SQL风格语法操作DF---------
    //创建临时视图
    df.createOrReplaceTempView("people")
    //编写SQL对临时视图进行查询
    spark.sql("select * from people where age>30").show()

    //---------通过DSL风格语法操作DF---------
    df.select("*").show()
    df.select(df("age") + 1).show()
   */
    //rdd=>df=>ds
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("zhangsan",20),("lisi",30)))

    //将rdd转换为df
    //如果不指定列名，没办法映射为DS
    //val df: DataFrame = rdd.toDF()
    val df: DataFrame = rdd.toDF("name","age")

    //如果指定的列名和样例类属性名不一致，没办法映射为DS。映射样例类的时候，是按照列的名字映射的
    //val df: DataFrame = rdd.toDF("username","age")

    val ds: Dataset[User01] = df.as[User01]
    //ds.show()

    //ds=>df=>rdd
    //val df1: DataFrame = ds.toDF()
    //val rdd1: RDD[Row] = df1.rdd
    //rdd1.foreach(println)

    //DS=>RDD
    val rdd2: RDD[User01] = ds.rdd

    //RDD=>DS
    val ds2: Dataset[User01] = rdd2.toDS()

  }
}

case class User01(name:String,age:Int)