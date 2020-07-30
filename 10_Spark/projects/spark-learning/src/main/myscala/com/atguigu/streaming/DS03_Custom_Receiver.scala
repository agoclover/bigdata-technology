package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{Socket}
import java.nio.charset.{StandardCharsets}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * <p>Spark Streaming Custom Receivers</p>
 *
 * <p>The following is a custom receiver that receives a stream of text over a socket.
 * It treats ‘\n’ delimited lines in the text stream as records and stores them with Spark.
 * If the receiving thread has any error connecting or receiving, the receiver is
 * restarted to make another attempt to connect.</p>
 *
 * @author Zhang Chao
 * @version Spark Streaming_day1
 * @date 2020/7/16 1:57 下午
 */
object DS03_Custom_Receiver {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val line: ReceiverInputDStream[String] = ssc.receiverStream(new CustomReceiver("localhost", 9999))

    line.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()

    ssc.awaitTermination()

    // 关闭连接
    sc.stop()
  }
}

/*
Receiver[T]  => store(something: T)
 */

class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    new Thread("My Receiver") {
      override def run() = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {
  }

  def receive(): Unit = {
    var socket: Socket = null
    var inputLine: String = null
    try {
      socket = new Socket(host, port)

      val reader: BufferedReader = new BufferedReader(
        new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

      inputLine = reader.readLine()

      while (!isStopped() && inputLine != null) {
        store(inputLine)
        inputLine = reader.readLine()
      }
      reader.close()
      socket.close()
      restart("try reconnect...")
    } catch {
      case e: java.net.ConnectException =>
        // connection error
        restart("Error on connecting.", e)
      case t: Throwable =>
        // other error
        restart("Error on others.", t)
    }
  }
}


//object DS03_Custom_Receiver {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomReceiver")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(10))
//
//    val words: ReceiverInputDStream[String] = ssc.receiverStream( // 相当于 RDD 里面是 String
//      new CustomReceiver("localhost", 9999))
//
//    val kv: DStream[(String, Int)] = words.flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//
//    kv.print()
//
//    ssc.start()
//
//    ssc.awaitTermination()
//  }
//}
//
//class CustomReceiver(host: String, port:Int)
//  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) with Logging{
//
//  override def onStart(): Unit = {
//    // Start the thread that receives data over a connection
//    new Thread("Socket receiver"){
//      override def run() = receive()
//    }.start()
//  }
//
//  override def onStop(): Unit = {
//
//  }
//
//  /** Create a socket connection and receive data until receiver is stopped */
//  def receive(): Unit ={
//    var socket: Socket = null
//    var inputLine: String = null
//    try {
//      socket = new Socket(host, port) // 是否应该由用 var
//      inputLine = new String
//
//      val reader = new BufferedReader(
//        new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
//
//      inputLine = reader.readLine()
//
//      while (!isStopped() && inputLine != null) {
//        store(inputLine)
//        inputLine = reader.readLine()
//      }
//
//      reader.close()
//      socket.close()
//
//      restart("restart the receiver...")
//    } catch {
//      case e : java.net.ConnectException =>
//        // restart if could not connect to server
//        restart(s"error to reconnect $host : $port", e)
//      case t : Throwable =>
//        // restart if other exception occurs
//        restart("Error receiving data.", t)
//    }
//  }
//}
