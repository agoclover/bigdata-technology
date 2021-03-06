package com.atguigu.sparkcore.d3conversionrate

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
 * @date 2020/7/11 10:54 下午
 */
object D3_Solution1 {
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
      )})

    // group by session_id
    // info(3)-info(6)-info(5)-info(4)  with info.action_time unordered
    val rdd3: RDD[(String, Iterable[UserActionInfo])] = rdd2.groupBy(_.session_id)

    val rdd4: RDD[(String, List[((Long,Long), Int)])] = rdd3.mapValues {
      datas => {
        // info(3)-info(4)-info(5)-info(6)  with info.action_time ordered
        val sortList: List[UserActionInfo] = datas.toList.sortBy(_.action_time)
        // 3-4-5-6
        val pageID: List[Long] = sortList.map(info => info.page_id)
        // 3-4-5-6 => (3,4), (4,5), (5,6) via collection.zip convert n elements to n-1 tuples
        val pageConvertion: List[(Long, Long)] = pageID.zip(pageID.tail)
        // tag (a,b) => ((a,b), 1) for aggregation
        val pageConvertionTag: List[((Long, Long), Int)] = pageConvertion.map((_, 1))
        // return ((a,b), 1)
        pageConvertionTag
      }
    } // => RDD[(String, List[((Long,Long), Int)])]


    val pageSum: RDD[((Long, Long), Int)] = rdd4.map(_._2)
      .flatMap(list => list)
      .reduceByKey(_ + _)

    val pageTotal: Map[Long, Long] = rdd2.map(info => (info.page_id, 1L))
      .reduceByKey(_ + _)
      .collect()
      .toMap

    pageSum.foreach{
      case ((page1,page2),sum) =>{
        val rate = sum.toDouble / pageTotal.getOrElse(page1, 1L)
        println(s"$page1-$page2=$rate")
      }
    }

    sc.stop()
  }
}
