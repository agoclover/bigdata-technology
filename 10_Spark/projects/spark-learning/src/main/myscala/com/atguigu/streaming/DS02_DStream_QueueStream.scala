package com.atguigu.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/16 11:17 上午
 */
object DS02_DStream_QueueStream {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(4))

    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    /*

    oneAtATime = true means (Whether) only one RDD should be consumed from the queue in every interval
    在 socket 流, 3s 会把 3s 内的数据封装成一个 RDD, 然后提交到 receiver 里面, 然后由 DStream process.
    这里, 因为其实是一次性把所有 RDD 添加到 queue 里的, 所以其实一次 (3s) 收集全了 RDD, 只不过一个 3s 只能处理一个 RDD, 所以
    隔三秒输出一次.

    如果改为 false, 那么 3s 内肯定收集完了, 然后直接会全部依次处理.(结果已证实)

    case 1: Seconds(3), onAtATime=false, queue += sc.makeRDD(1 to 5) * 10
          首先, 产生数据在瞬间, queue 里就有 10 个 RDD[Int];
          3s 收集一次, 则会第一次就收集所有的 RDD[Int];
          一次计算所有 RDD;
          计算时 10 个 RDD[Int] 数据结构相同, 合并;
          输出 (1,10) (2,10) (3,10) (4,10) (5,10)

    case 2: Seconds(3), onAtATime=true, queue += sc.makeRDD(1 to 5) * 10
          首先, 产生数据在瞬间, queue 里就有 10 个 RDD[Int];
          3s 收集一次, 则会第一次就收集所有的 RDD[Int];
          一次计算一个 RDD, 计算并输出;
          因为有 10 个 RDD, 所以需要 10 个 3s 的间隔;
          输出 (1,1) (2,1) (3,1) (4,1) (5,1)
              (1,1) (2,1) (3,1) (4,1) (5,1)... * 10

    case 3: Seconds(4), onAtATime=false, queue += sc.makeRDD(1 to 5) sleep(2) * 10
          一个间隔 4s 产生 2 个 RDD, 然后被收集一次;
          一次处理所有收集到的未处理的 RDDs, 每次处理两个, 两个的数据合并;
          输出 (1,2) (2,2) (3,2) (4,2) (5,2)
              (1,2) (2,2) (3,2) (4,2) (5,2)... * 5
              当然可能出现 (1,1) (2,1) (3,1) (4,1) (5,1)
     */
    val inputStream: InputDStream[Int] = ssc.queueStream[Int](queue, false)

    val keyCounts: DStream[(Int, Int)] = inputStream.map((_, 1))
      .reduceByKey(_ + _)

    keyCounts.print()

    ssc.start()

    for(i <- 1 to 10){
      queue += sc.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
