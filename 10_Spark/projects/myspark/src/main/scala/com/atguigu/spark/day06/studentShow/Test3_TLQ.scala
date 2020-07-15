package com.atguigu.spark.day06.studentShow

//import com.atguigu.spark.day04.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test3_TLQ {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("map")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\spark-core数据\\user_visit_action.txt")

    //session 阶段  pageID 页面  时间
    /*
        val sessionRdd: RDD[(String, String, String)] = rdd.map {
          line =>
            val strings = line.split("_")
            (strings(2), strings(3), strings(4), strings(6), strings(7))
        }.filter(_._4 != null).filter(_._5 != "-1").map {
          case (session, page, time, str3, str4) =>
            (session, time, page)
        }

     */
    val sessionRdd: RDD[(String, String, String)] = rdd.map {
      line =>
        val strings = line.split("_")
        (strings(2), strings(3), strings(4), strings(6), strings(7))
    }.map {
      case (session, page, time, str3, str4) =>
        (session, time, page)
    }

    //组内降序排序
    val pageRdd: RDD[(String, Int)] = sessionRdd.groupBy(_._1).sortBy(_._2.foreach {
      case (str, str1, str2) => str1
    }, false).flatMap(t => t._2).map {
      case (session, str1, page) => (page, 1)
    }

    val pageZipRdd: RDD[(String, Int)] = sessionRdd.groupBy(_._1).mapValues {
      datas => {
        val sortRdd = datas.toList.sortBy(_._2).reverse
        val pageList = sortRdd.map(t => (t._3, 1))
        pageList.zip(pageList.tail).map {
          case ((page1, _), (page2, _)) =>
            (page1 + "-" + page2, 1)
        }
      }
    }.map(_._2).flatMap(list => list).reduceByKey(_ + _)
    //每个页面出现多少次
    val pageNum: RDD[(String, Int)] = sessionRdd.map(t => (t._3, 1)).reduceByKey(_ + _)

    val pageNumMap: Map[String, Int] = pageNum.collect().toMap
   /*
    val resRdd: RDD[(String, Double)] = pageZipRdd.map {
      case (pages, sum) => {
        val strings = pages.split("-")
        val sum2 = pageNumMap.getOrElse(strings(0), 1)
        (pages, sum.toDouble / sum2)
      }
    }

    */
    pageZipRdd.foreach{
      case (pages, sum) => {
        val strings = pages.split("-")
        val sum2 = pageNumMap.getOrElse(strings(0), 1)
        println(pages + "=" + sum.toDouble / sum2)
      }
    }

    //每个跳转多少次
    /*
        val pagRddList: List[(String, Int)] = pageRdd.collect().toList

        val jumpList: List[(String, Int)] = pagRddList.zip(pagRddList.tail).map {
          case ((pageId1, _), (pageId2, _)) => {
            (pageId1 + "-" + pageId2, 1)
          }
        }
        val jumpRdd: RDD[(String, Int)] = sc.makeRDD(jumpList).reduceByKey(_ + _)


        val resRdd: RDD[(String, Double)] = jumpRdd.map {
          case (page, num) =>
            val strings = page.split("-")
            val sum = pageNumMap.getOrElse(strings.head, 1)
            (page, num.toDouble / sum)
        }

     */

  }
}
