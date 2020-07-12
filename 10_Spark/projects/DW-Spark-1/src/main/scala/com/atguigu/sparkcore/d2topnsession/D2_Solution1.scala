package com.atguigu.sparkcore.d2topnsession

import com.atguigu.sparkcore.bean.UserActionInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/11 10:02 下午
 */
object D2_Solution1 {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    // 15, 2, 20, 12, 11, 17, 7, 9, 19, 13
    val list = List[Long](15, 2, 20, 12, 11, 17, 7, 9, 19, 13)

    // read datas
    val rdd1: RDD[String] = sc.textFile("/Users/amos/projects/data_warehouse/Spark/DW-Spark-1/input")
    // split line to a array, then wrap them to a UserActionInfo instance
    val rdd2: RDD[UserActionInfo] = rdd1.map(line => {
      val arr: Array[String] = line.split("_")
      UserActionInfo(
        arr(0),
        arr(1).toLong,
        arr(2),
        arr(3).toLong,
        arr(4),
        arr(5),
        arr(6).toLong,
        arr(7).toLong,
        arr(8),
        arr(9),
        arr(10),
        arr(11),
        arr(12).toLong
      )
    }
    )
    // filter the data
    val rdd3: RDD[UserActionInfo] = rdd2.filter(info => list.contains(info.click_category_id))
    // map info to tuple ((category, session_id), 1) to sum
    val rdd4: RDD[((Long, String), Int)] = rdd3.map(info => ((info.click_category_id, info.session_id), 1))
    // sum
    val rdd5: RDD[((Long, String), Int)] = rdd4.reduceByKey(_ + _)
    // map tuple to (category, (session_id, sum))
    val rdd6: RDD[(Long, (String, Int))] = rdd5.map {case ((category, session), count) => (category, (session, count)) }
    // group by category
    val rdd7: RDD[(Long, Iterable[(String, Int)])] = rdd6.groupByKey()
    // sort and get top 10 session_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           c
    val rdd8: RDD[(Long, List[(String, Int)])] = rdd7.mapValues(iter => iter.toList.sortBy(-1 * _._2).take(10))

    rdd8.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
