package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Felix
  * Date: 2020/7/10
  * Desc: 
  */
object Spark08_TopN {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    /*
    //方案1
    //读取外部文件创建RDD     时间戳，省份，城市，用户，广告
    val lineRDD: RDD[String] = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\agent.log")

    //对读取到的数据，进行结构的转换  (省份-广告,1)
    val mapRDD: RDD[(String, Int)] = lineRDD.map(
      line => {
        //对读取到的行数据进行切割
        val fields: Array[String] = line.split(" ")
        (fields(1) + "-" + fields(4), 1)
      }
    )
    //按照省份和广告进行分组
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    //获得当前省份和广告 总点击数
    val mapRDD1: RDD[(String, Int)] = groupRDD.map {
      case (proAd, datas) => {
        (proAd, datas.size)
      }
    }

    //对RDD再次进行结构的转换
    val mapRDD2: RDD[(String, (String, Int))] = mapRDD1.map {
      case (proAd, count) => {
        (proAd.split("-")(0), (proAd.split("-")(1), count))
      }
    }

    //按照省份进行分组，将相同的省份放在一起
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    //对当前省份广告点击次数降序排序  取前3
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD2.mapValues(
      datas => datas.toList.sortBy(-_._2).take(3)
    )

    resRDD.collect().foreach(println)
*/
    //方案2
    //读取外部文件创建RDD     时间戳，省份，城市，用户，广告
    val lineRDD: RDD[String] = sc.textFile("E:\\Felix课程\\大数据\\大数据_200317\\Felix_03_尚硅谷大数据技术之Spark\\2.资料\\agent.log")

    //对读取到的数据，进行结构的转换  (省份-广告,1)
    val mapRDD: RDD[(String, Int)] = lineRDD.map(
      line => {
        //对读取到的行数据进行切割
        val fields: Array[String] = line.split(" ")
        (fields(1) + "-" + fields(4), 1)
      }
    )

    //计算省份广告点击次数  (省份-广告,10)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

    //对RDD再次进行结构的转换  (省份,(广告,10))
    val mapRDD2: RDD[(String, (String, Int))] = reduceRDD.map {
      case (proAd, count) => {
        (proAd.split("-")(0), (proAd.split("-")(1), count))
      }
    }

    //按照省份进行分组，将相同的省份放在一起
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    //对当前省份广告点击次数降序排序  取前3
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD2.mapValues(
      datas => datas.toList.sortWith(
        (left,right)=>{
          left._2 > right._2
        }
      ).take(3)
    )

    resRDD.collect().foreach(println)
    // 关闭连接
    sc.stop()
  }
}
