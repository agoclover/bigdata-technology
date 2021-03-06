package com.atguigu.spark.day06.studentShow
import com.atguigu.spark.day06.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/11 10:54 下午
 */
object D3_Solution2_ZC {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    // read datas
    val lineRDD: RDD[String] = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")

    // split line to a array, then wrap them to a UserActionInfo instance
    val infoRDD: RDD[UserVisitAction] = lineRDD.map(line => {
      val arr: Array[String] = line.split("_")
      UserVisitAction(
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


    /*
    // => info
    val cvtSumRDD: RDD[((Long, Long), Int)] = infoRDD.groupBy(_.session_id)
      .map(_._2)
      .map(_.toList.sortBy(_.action_time))
      .map(info => info.map(_.page_id)) //RDD[List[Long]] => ((page, pageNext),1)
      .flatMap(listPage => {
      val listCvtUnit: ListBuffer[((Long, Long), Int)] = ListBuffer()
      for (i <- 1 until listPage.size) {
        listCvtUnit.append(((listPage(i - 1), listPage(i)), 1))
      }
      listCvtUnit
    }
    ).reduceByKey(_+_)

    val pageSumMap: Map[Long, Long] = infoRDD.map(info => (info.page_id, 1L))
      .reduceByKey(_ + _)
      .collect()
        .toMap

    //RDD[((Long, Long), Int)]
    val resRDD: RDD[(Double, (Long, Long))] = cvtSumRDD.map {
      case ((page, pageNext), count) => {
        val rate: Double = count.toDouble / pageSumMap.getOrElse(page, 1L)
        (rate, (page, pageNext))
      }
    }*/

    sc.stop()

    /*// group by session_id
    // info(3)-info(6)-info(5)-info(4)  with info.action_time unordered
    val sessionInfoRDD: RDD[(String, Iterable[UserVisitAction])] = infoRDD.groupBy(_.session_id)
    // erase unusable session message
    val iterableInfoRDD: RDD[Iterable[UserVisitAction]] = sessionInfoRDD.map(_._2)

    // map every
    val cvtUnitRDD: RDD[((Long,Long), Int)] = iterableInfoRDD.flatMap {
      iterableInfo => {
        // info(3)-info(4)-info(5)-info(6)  with info.action_time ordered
        val sortList: List[UserVisitAction] = iterableInfo.toList.sortBy(_.action_time)
        // 3-4-5-6
        val pageID: List[Long] = sortList.map(info => info.page_id)
        // 3-4-5-6 => (3,4), (4,5), (5,6) via collection.zip convert n elements to n-1 tuples
        val pageConvertion: List[(Long, Long)] = pageID.zip(pageID.tail)
        // tag (a,b) => ((a,b), 1) for aggregation
        val pageConvertionTag: List[((Long, Long), Int)] = pageConvertion.map((_, 1))
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
    sc.stop()*/
  }
}
