package com.atguigu.sparkcore.d3conversionrate

import com.atguigu.sparkcore.bean.UserActionInfo
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
object D3_SolutionDemo {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 读取数据
    val dataRDD: RDD[String] = sc.textFile("/Users/amos/BigdataLearn/10_Spark/Projects/DW-Spark-1/input/user_visit_action.txt")

    //将原始数据进行转换(分解)
    val actionRDD: RDD[UserActionInfo] = dataRDD.map {
      line => {
        val fields: Array[String] = line.split("_")
        UserActionInfo(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    }

    //*************需求三*****************

    //计算分母-将相同的页面id进行聚合统计(pageId,sum)
    val pageIdRDD: RDD[(Long, Long)] = actionRDD.map(action => {
      (action.page_id, 1L)
    })
    val fmIdsMap: Map[Long, Long] = pageIdRDD.reduceByKey(_+_).collect().toMap

    //计算分子-将页面id进行拉链，形成连续的拉链效果，转换结构(pageId-pageId2,1)
    //将原始数据根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserActionInfo])] = actionRDD.groupBy(_.session_id)
    //将分组后的数据根据时间进行排序（升序）
    var pageFlowRDD: RDD[(String, List[(String, Int)])] = sessionRDD.mapValues(datas => {
      val actions: List[UserActionInfo] = datas.toList.sortWith(
        (left, right) => {
          left.action_time < right.action_time
        }
      )
      //将排序后的数据进行结构的转换(pageId,1)
      val pageIdToOneList: List[(Long, Int)] = actions.map(action => (action.page_id, 1))
      val pageFlows: List[((Long, Int), (Long, Int))] = pageIdToOneList.zip(pageIdToOneList.tail)
      pageFlows.map {
        case ((pageId1, _), (pageId2, _)) => {
          (pageId1 + "-" + pageId2, 1)
        }
      }
    }
    )

    //将转换结构后的数据进行聚合统计(pageId-pageId2,sum)
    val pageFlowMapRDD: RDD[(String, Int)] = pageFlowRDD.map(_._2).flatMap(list=>list)
    val page1AndPage2ToSumRDD: RDD[(String, Int)] = pageFlowMapRDD.reduceByKey(_+_)

//    //计算页面单跳转换率
//    page1AndPage2ToSumRDD.foreach{
//      case (pageFlow,fz)=>{
//        val pageIds: Array[String] = pageFlow.split("-")
//        //为了避免分母不存在，这里默认值给1
//        val fmSum: Long = fmIdsMap.getOrElse(pageIds(0).toLong,1L)
////        println(pageFlow + "=" + fz.toDouble/fmSum)
//        println(fz.toDouble/fmSum)
//      }
//    }

//    page1AndPage2ToSumRDD.map{
//      case (pageFlow,fz)=>{
//        val pageIds: Array[String] = pageFlow.split("-")
//        //为了避免分母不存在，这里默认值给1
//        val fmSum: Long = fmIdsMap.getOrElse(pageIds(0).toLong,1L)
//        fz.toDouble/fmSum
//      }
//    }.sortBy(-1 * _).collect().foreach(println)

    page1AndPage2ToSumRDD.foreach{
      case (pageFlow,fz)=>{
        val pageIds: Array[String] = pageFlow.split("-")
        //为了避免分母不存在，这里默认值给1
        val fmSum: Long = fmIdsMap.getOrElse(pageIds(0).toLong,1L)
        println(pageFlow + "=" + fz.toDouble/fmSum)
      }
    }

    // 关闭连接
    sc.stop()
  }
}
