package com.atguigu.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream} // not necessary since Spark 1.3

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/16 9:44 上午
 */
object DS01_Test {
  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

//    val sc = new SparkContext(conf) //  existing SparkContext
//    val ssc = new StreamingContext(sc, Seconds(3))

//    val sparkContext: SparkContext = ssc.sparkContext

    val ssc = new StreamingContext(conf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val kv: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    kv.print()

    // Start the computation
    ssc.start()

    ssc.awaitTermination()  // Wait for the computation to terminate

    ssc.stop(false) // To stop only the StreamingContext, set the optional parameter of stop() called stopSparkContext to false.
  }
}
