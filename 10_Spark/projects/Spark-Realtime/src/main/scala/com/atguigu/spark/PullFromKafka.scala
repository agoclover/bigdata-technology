package com.atguigu.spark

import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/18 3:37 下午
 */
object PullFromKafka extends Serializable {

  import java.text.SimpleDateFormat
  val sdfDay = new SimpleDateFormat("yyyy-MM-dd")
  val sdfMin = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
  var ssc:StreamingContext = null

  def getValue(interval:Int, cpPath:String):DStream[String]={
    ssc = new StreamingContext(conf, Seconds(interval))
    ssc.sparkContext.setCheckpointDir(cpPath)

    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "ad_log_0317"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      //位置策略，指定计算的Executor
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams))

    //测试Kafka中消费数据
    val resRDD: DStream[String] = kafkaDS.map(_.value())
    resRDD
  }

  def getSSC():StreamingContext = ssc
  def getSC():SparkContext = ssc.sparkContext

  def dateFormat(line:String):UserAdAction2={
    val arr: Array[String] = line.split(",")
    val timeStamp: Long = arr(0).toLong
    val day: String = sdfDay.format(timeStamp)
    val min: String = sdfMin.format(timeStamp)
    val action: UserAdAction2 = UserAdAction2(timeStamp,
      day,
      min,
      arr(1),
      arr(2),
      arr(3).toInt,
      arr(4).toInt
    )
    action
  }


}
