package com.atguigu.myspark.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/8 2:22 下午
 */
object S05_Transformation_changePartition {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val RDD = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    RDD.mapPartitionsWithIndex{
      (index, datas) => {
        println(index + " --> " + datas.mkString(", "))
        datas
      }
    }.collect

    println("------")

    // change the number of partitions by calling coalesce method
    /*
    narrowing partitions without shuffle by default which has high efficiency but
    may result to data skewing.
     */
    val newRDD = RDD.coalesce(2)

    newRDD.mapPartitionsWithIndex{
      (index, datas) => {
        println(index + " --> " + datas.mkString(", "))
        datas
      }
    }.collect

    println("------")

    // enlarging partitions without shuffle by default
    val newRDD2 = RDD.coalesce(4)

    newRDD2.mapPartitionsWithIndex{
      (index, datas) => {
        println(index + " --> " + datas.mkString(", "))
        datas
      }
    }.collect

    println("------")

    /*
    Actually, use .repartition instead to enlarge partitions
    which call coalesce inside with parameter shuffle always
    equals to true.
     */

    val reRDDN = RDD.repartition(2)

    // 关闭连接
    sc.stop()
  }
}
