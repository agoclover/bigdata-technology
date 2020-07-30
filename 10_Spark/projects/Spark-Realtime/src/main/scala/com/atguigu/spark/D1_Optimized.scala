package com.atguigu.spark

import org.apache.spark.streaming.dstream.DStream

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/18 3:40 下午
 */
object D1_Optimized {
  def main(args: Array[String]): Unit = {
    val interval :Int = 3
    val cpPath: String = "/Users/amos/BigdataLearn/10_Spark/projects/Spark-Realtime/cp_d1"

    val value: DStream[String] = PullFromKafka.getValue(interval, cpPath)

    val actionUnit: DStream[((String, String, Int), Int)] = value.map(line => {
      val action: UserAdAction2 = PullFromKafka.dateFormat(line)
      ((action.day, action.area, action.ad_id), 1)
    })

    val actionCounts: DStream[((String, String, Int), Int)] = actionUnit.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => Option(seq.sum + state.getOrElse(0))
    )

    actionCounts.map {
      case ((day, area, ad), counts) => ((day, area, counts), ad)
    }.foreachRDD(RDD =>{
      println("-------------------------------------------")
      println("Day\t\t\tArea\tAd\tCounts")
      RDD.sortByKey(false)
        .map{case ((day, area, ad), counts) => s"$day\t$area\t\t$ad\t$counts"}
        .collect()
        .foreach(println)
      println()
    }
    )

    PullFromKafka.getSSC().start()
    PullFromKafka.getSSC().awaitTermination()
  }
}
