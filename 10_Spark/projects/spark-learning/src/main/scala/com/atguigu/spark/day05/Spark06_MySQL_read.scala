package com.atguigu.spark.day05

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/11
  * Desc: 从MySQL数据库中读取数据
  */
object Spark06_MySQL_read {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过JdbcRDD读取Mysql数据库中的数据
    //sc: SparkContext,   Spark上下文
    //getConnection: () => Connection,  获取连接的函数
    //sql: String,    执行的SQL语句
    //lowerBound: Long,   下限
    //upperBound: Long,   上限
    //numPartitions: Int, 分区数
    //mapRow: (ResultSet) 结果集
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop202:3306/test"
    val userName = "root"
    val passWd = "123456"

    var sql:String = "select * from user where id>=? and id<=?"
    val jdbcRDD: JdbcRDD[(Int, String, Int)] = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      sql,
      1,
      20,
      2,
      rs => {
        (rs.getInt(1), rs.getString(2), rs.getInt(3))
      }
    )

    jdbcRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
