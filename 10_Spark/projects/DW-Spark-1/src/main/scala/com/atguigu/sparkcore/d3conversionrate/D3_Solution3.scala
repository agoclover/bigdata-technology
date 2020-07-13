package com.atguigu.sparkcore.d3conversionrate

import com.atguigu.sparkcore.bean.UserActionInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * <p>Title: </p>
 *
 * <p>核心代码处不用 zip 用 for(index < 1 until size) </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/11 10:54 下午
 */
object D3_Solution3 {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    // read datas
    val lineRDD: RDD[String] = sc.textFile("/Users/amos/BigdataLearn/10_Spark/Projects/DW-Spark-1/input/user_visit_action.txt")

    // split line to a array, then wrap them to a UserActionInfo instance
    val infoRDD: RDD[UserActionInfo] = lineRDD.map(line => {
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
    val sessionInfoRDD: RDD[(String, Iterable[UserActionInfo])] = infoRDD.groupBy(_.session_id)
    // erase unusable session message
    val iterableInfoRDD: RDD[Iterable[UserActionInfo]] = sessionInfoRDD.map(_._2)

    // map every
    val cvtUnitRDD: RDD[((Long,Long), Int)] = iterableInfoRDD.flatMap {
      iterableInfo => {
        // info(3)-info(4)-info(5)-info(6)  with info.action_time ordered
        val sortList: List[UserActionInfo] = iterableInfo.toList.sortBy(_.action_time)
        // 3-4-5-6
        val pageID: List[Long] = sortList.map(info => info.page_id)
        // 3-4-5-6 => (3,4), (4,5), (5,6) via collection.zip convert n elements to n-1 tuples

        val pageConvertionTag : ListBuffer[((Long, Long), Int)] = ListBuffer()

        for (index <- 1 until pageID.size) {
          pageConvertionTag.append(((pageID(index-1), pageID(index)), 1))
        }

        // return ((a,b), 1)
        pageConvertionTag
      }
    } // => RDD[((Long,Long), Int)]

    // get the sum of every conversion for computing
    val cvtSumRDD: RDD[((Long, Long), Int)] = cvtUnitRDD.reduceByKey(_ + _)

    // get the sum of every page accessed as a map for computing
    val pageSum: Map[Long, Long] = infoRDD // RDD[UserActionInfo]
      .map(info => (info.page_id, 1L))     // RDD[(Long, Long)]
      .reduceByKey(_ + _)                  // RDD[(Long, Long)]
      .collect                             // Array[(Long, Long)]
      .toMap                               // Map[(Long, Long)]

    //    cvtSumRDD.foreach{
    //      case ((page,pageNext),sum) =>{
    //        val rate :Double = sum.toDouble / pageSum.getOrElse(page, 1L)
    //        val roundedRate: String = rate.formatted("%.5f")
    //        println(s"$page->$pageNext rates $roundedRate")
    //      }
    //    }

    // calculate the rate of page conversions
    val ResRDD: RDD[(Double, (Long, Long))] = cvtSumRDD.map { case ((page, pageNext), sum) => {
      val rate: Double = sum.toDouble / pageSum.getOrElse(page, 1L)
      (rate, (page, pageNext))
    }
    }

    // sort the results in descending order then format them
    ResRDD
      .sortBy(-1 * _._1)
      .collect
      .foreach{ case (rate, (page, pageNext)) => {
        val roundedRate: String = rate.formatted("%.5f")
        println(s"$page -> $pageNext rates $roundedRate")
      }
      }
    // stop the application
    sc.stop()
  }
}
