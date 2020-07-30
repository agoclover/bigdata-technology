package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver


/*

 */

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/16 4:25 下午
 */
object DS04_Custom_ReceiverDemo {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val line: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("localhost", 9999))

    line.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()

    ssc.awaitTermination()

    // 关闭连接
    sc.stop()
  }
}

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //创建一个Socket
  private var socket: Socket = _

  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }

  //读数据并将数据发送给Spark
  def receive(): Unit = {
    try {
      socket = new Socket(host, port)
      //创建一个BufferedReader用于读取端口传来的数据
      val reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream, StandardCharsets.UTF_8))
      //定义一个变量，用来接收端口传过来的数据
      var input: String = null

      //读取数据 循环发送数据给Spark 一般要想结束发送特定的数据 如："==END=="
      while ((input = reader.readLine())!=null) {
        store(input)
      }
    } catch {
      case e: ConnectException =>
        restart(s"Error connecting to $host:$port", e)
        return
    }
  }

  override def onStop(): Unit = {
    if(socket != null ){
      socket.close()
      socket = null
    }
  }
}