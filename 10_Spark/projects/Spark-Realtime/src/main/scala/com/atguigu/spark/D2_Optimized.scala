package com.atguigu.spark

import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/18 4:37 下午
 */
object D2_Optimized {
  def main(args: Array[String]): Unit = {
    val interval :Int = 6
    val cpPath: String = "/Users/amos/BigdataLearn/10_Spark/projects/Spark-Realtime/cp_d2"

    val value: DStream[String] = PullFromKafka.getValue(interval, cpPath)

    val actionUnit: DStream[((String, Int), Int)] = value.map(line => {
      val action: UserAdAction2 = PullFromKafka.dateFormat(line)
      ((action.min, action.ad_id), 1)
    })

    val windowDS: DStream[((String, Int), Int)] = actionUnit.window(Minutes(60), Seconds(6))

    windowDS.reduceByKey(_+_)
      .map{case ((min,ad),counts)=>(min,(ad, counts))}
      .groupByKey()
      .foreachRDD(RDD=>{
        println("-----------------------------------")
        println("Minute\t\tAd & Counts")
        RDD.mapValues(_.toList.sortBy(-_._2).mkString(", "))
          .sortByKey()
          .collect()
          .foreach(println)
      })

    PullFromKafka.getSSC().start()
    PullFromKafka.getSSC().awaitTermination()
  }
}
