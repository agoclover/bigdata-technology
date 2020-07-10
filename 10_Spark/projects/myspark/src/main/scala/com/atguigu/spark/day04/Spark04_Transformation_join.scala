package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-join
  *   回顾SQL连接分类
  *     -按照年代分区
  *       *SQL92
  *         select * from dept d,emp e where d.deptno = e.deptno
  *       *SQL99
  *         select * from dept d join emp e on d.deptno = e.deptno
  *     -按照连接方式
  *       *内连接：两张表进行关联查询，将两张表中完全匹配的记录查询出来
  *         >等值连接
  *         >非等值连接
  *         >自连接
  *       *外连接：两张表进行关联查询，将其中一张表的记录全部查询出来，那么另外一张表肯定会有数据无法
  *         与其匹配，会自动模拟出空值进行匹配
  *         >左（外）连接
  *         >右（外）连接
  *         >全连接
  */
object Spark04_Transformation_join {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))

    //3.2 创建第二个pairRDD
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6),(2,7)))

    //val newRDD: RDD[(Int, (String, Int))] = rdd.join(rdd1)
    //val newRDD: RDD[(Int, (Int, String))] = rdd1.join(rdd)
    //val newRDD: RDD[(Int, (String, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    //val newRDD: RDD[(Int, (Option[String], Int))] = rdd.rightOuterJoin(rdd1)
    //val newRDD: RDD[(Int, (Option[String], Option[Int]))] = rdd.fullOuterJoin(rdd1)

    val newRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)
    newRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
