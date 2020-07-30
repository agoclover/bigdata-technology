package com.atguigu.myspark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 11:20 上午
 */
object S03_Transformation_filter {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 3)
        .filter(_%2 == 0)
        .collect
        .foreach(println _)

    // 关闭连接
    sc.stop()
  }
}
