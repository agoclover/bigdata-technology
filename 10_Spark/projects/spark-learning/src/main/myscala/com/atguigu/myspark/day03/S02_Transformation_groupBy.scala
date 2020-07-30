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
 * @date 2020/7/8 10:05 上午
 */
object S02_Transformation_groupBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

//    val gRDD = sc.makeRDD(1 to 10, 2)
//      .groupBy(_ % 2) // 有 shuffle 的过程
//
//    gRDD.mapPartitionsWithIndex((index, datas) => {
//      println(index + "--->" + datas.mkString(", "))
//      datas
//    }).collect()

//    Thread.sleep(Long.MaxValue)

//    val res = wc(sc, List("hello hello", "good good", "atguigu amos"))
//    res.foreach(println)

    val res: Array[(String, Int)] = wcc(sc, List(("hello thank you thank you", 3), ("amos hello hello amos", 3), ("hello amos cathy thank", 4)))
    res.foreach(println _)

    // 关闭连接
    sc.stop()
  }

  /**
   * word count 案例
   * @param sc
   * @param list
   * @return
   */
  def wc(sc: SparkContext, list: List[String])={
    sc.makeRDD(list)
      .flatMap(_.split(" "))
      .groupBy(word => word)
      .map{case (word, datas) => (word, datas.size)}
      .collect
  }

  /**
   * 复杂版 word count
   * @param sc
   * @param list
   * @return
   */
  def wcc(sc: SparkContext, list: List[(String,Int)])={
    sc.makeRDD(list)
      .flatMap{case (word, count) => word.split(" ").map((_, count))}
      .groupBy(_._1)
      .map{case (word, countList) => (word, countList.map(_._2).sum)}
      .collect
  }
}
