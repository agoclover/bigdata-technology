package com.atguigu.myspark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 2:38 下午
 */
object S06_Transformation_sortBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    val RDD = sc.makeRDD(List(6, 4, 1, 6, 8, 2, 4, 7, 2, 8, 9), 2)
    val RDD = sc.makeRDD(List((1,2), (4,3), (9,4),(3,4),(5,5)), 2)

    RDD.sortBy(elem => elem, false)
      .collect
      .foreach(println)

    // 关闭连接
    sc.stop()
  }
}
