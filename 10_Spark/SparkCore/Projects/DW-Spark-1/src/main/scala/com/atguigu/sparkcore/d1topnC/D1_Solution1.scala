package com.atguigu.sparkcore.d1topnC

import com.atguigu.sparkcore.bean.UserActionInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * <p>需求一的第一种方法</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version spark_day5
 * @date 2020/7/11 7:14 下午
 */
object D1_Solution1{

  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)
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
    // transform info to tuple
    val rdd3: RDD[(Long, (Int, Int, Int))] = rdd2.flatMap(infoToTuple)

    // aggregate the counts by key
    val rdd4: RDD[(Long, (Int, Int, Int))] = rdd3.reduceByKey(
      (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    )

    // sort
    val rdd5: RDD[(Long, (Int, Int, Int))] = rdd4.sortBy(
      CCount => (CCount._2._1, CCount._2._2, CCount._2._3),
      false)

    // get the results and print them
    val res: Array[(Long, (Int, Int, Int))] = rdd5.take(10)

    println(res.mkString("\n"))

    Thread.sleep(Long.MaxValue)

    // 关闭连接
    sc.stop()
  }

  /**
   * 将一条信息转换为一个 ListBuffer 集合, 这个集合存储商品品类的元组
   * @param info
   * @return
   */
  def infoToTuple(info: UserActionInfo): ListBuffer[(Long, (Int, Int, Int))] = {
    val list : ListBuffer[(Long, (Int, Int, Int))] = new ListBuffer[(Long, (Int, Int, Int))]

    if (info.click_category_id != -1L) {
      list.append((info.click_category_id, (1,0,0)))
    }

    // 这里可以尝试封装
    if (!("null".equals(info.order_category_ids))) {
      info.order_category_ids
        .split(",")
        .foreach(str => list.append((str.toLong, (0,1,0))))
    }

    if (!("null".equals(info.pay_category_ids))) {
      info.pay_category_ids
        .split(",")
        .foreach(str => list.append((str.toLong, (0,0,1))))
    }
    list
  }
}
