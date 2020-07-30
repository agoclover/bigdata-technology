package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/7
  * Desc: 转换算子-combineByKey 转换结构后，对分区内和分区间数据进行操作
  */
object Spark01_Transformation_combineByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //需求：求出每一个学生的平均成绩
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("tlq",90),("tlq",80),("tlq",85),("wj",80),("wj",100),("wj",90)))

    /*
    //方案1   groupByKey
    //按照学生姓名，对学生进行分组
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //对上面的RDD进行结构的转换
    val resRDD: RDD[(String, Int)] = groupRDD.map {
      case (name, datas) => {
        (name, datas.sum / datas.size)
      }
    }

    //方案2   reduceByKey
    //对课程的门数进行记录
    val mapRDD: RDD[(String, (Int, Int))] = rdd.map {
      case (name, score) => {
        (name, (score, 1))
      }
    }

    //对分数和门数进行聚合
    val reduceRDD: RDD[(String, (Int, Int))] = mapRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2)
    })

    //通过map进行结构转换，求出平均成绩
    val resRDD: RDD[(String, Int)] = reduceRDD.map {
      case (name, (scoreSum, countSum)) => {
        (name, scoreSum / countSum)
      }
    }
   */
    //方案3   使用combineByKey完成
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      num => (num, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (tup1: (Int, Int), tup2: (Int, Int)) => {
        (tup1._1 + tup2._1, tup1._2 + tup2._2)
      }
    )

    //求平均成绩
    val resRDD: RDD[(String, Int)] = combineRDD.map {
      case (name, (scoreSum, countSum)) => {
        (name, scoreSum / countSum)
      }
    }

    resRDD.collect().foreach(println)

    // 关闭连接
    sc.stop()
  }
}
