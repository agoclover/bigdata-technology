package com.atguigu.myspark.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Serializable</p>
 *
 * <p>
 * Why serialize?
 * - spark program on driver while operator on executor.
 *   if executor need access data in driver, That involves
 *   cross-process or cross-node data transfer, thus serialize needed.
 *   </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/10 2:30 下午
 */
object S05_Serializable {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val u1 = new User("u1")
    val u2 = new User("u2")

    val userRDD = sc.makeRDD(List(u1, u2))
    userRDD.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
class User(var name:String) extends Serializable {
  override def toString: String = name
}
