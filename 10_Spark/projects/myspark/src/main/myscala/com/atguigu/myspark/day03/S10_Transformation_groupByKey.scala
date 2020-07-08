package com.atguigu.myspark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 4:03 下午
 */
object S10_Transformation_groupByKey {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val RDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 5), ("b", 3), ("c", 2), ("a", 1), ("b", 2)))

    // 两者返回的 RDD 的 v 不同
    val newRDD: RDD[(String, Iterable[Int])] = RDD.groupByKey()

    val newRDD2: RDD[(String, Iterable[(String, Int)])] = RDD.groupBy(_._1)

    newRDD.map{case (key, datas)=>(key, datas.sum)}
        .collect()
        .foreach(println)

    /*
    在不影响业务的条件下, 优先使用 operator reduceByKey
     */

    // 关闭连接
    sc.stop()
  }
}
