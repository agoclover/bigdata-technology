package com.atguigu.myspark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 2:03 下午
 */
object S04_Transformation_distinct {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 3, 4, 4, 5, 6, 7, 8, 9, 9), 3)

    rdd.mapPartitionsWithIndex{
      (index, datas) => {
        println(index + "-->" + datas.mkString(", "))
        datas
      }
    }.collect

    // 默认产生 RDD 分区和原 RDD 分区个数相同; 也可以传入一个整数作为新的分区数
    val newRDD = rdd.distinct(2)

    newRDD.mapPartitionsWithIndex{
      (index, datas) => {
        println(index + "-->" + datas.mkString(", "))
        datas
      }
    }.collect

    // 关闭连接
    sc.stop()
  }
}
