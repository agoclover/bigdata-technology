package com.atguigu.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/16 10:07 下午
 */
object DS05_Kafka_Integration {
  def main(args: Array[String]): Unit = {
    // create SparkConf and set App's name
    val conf: SparkConf = new SparkConf().setAppName("SparkStreamingTest").setMaster("local[*]")

    // create SparkContext which is the submission entrance of Spark App
    val sc: SparkContext = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParameters: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test-consumer-group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    /**
     * ↓ explain parameters:
     * scc: StreamingContext
     * locationStrategy: LocationStrategy => [[LocationStrategies]]
     *
     * 1.kafka作为集群运行在一个或者多个服务器上
     * 2.kafka集群存储的消息是以topic为类别记录的
     * 3.kafka存储的消息是k-v键值对，k是offset偏移量，v就是消息的内容
     * 4.topic：kafka将消息分门别类，每一类的消息称之为topic
     * 5.broker：已发布的消息保存在一组服务器中，称之为Kafka集群。集群中的每一个服务器都是一个代理(Broker). 消费者可以订阅一个或多个主题（topic），并从Broker拉数据，从而消费这些已发布的消息。
     * 6.消息：kafka会保存消息直到它过期，无论是否被消费了。
     * 7.producer：发布消息的对象，往某个topic中发布消息，也负责选择发布到topic中的哪个分区
     * 8.consumer：订阅消息并处理发布的消息的对象
     *
     * 9.patition：topic是逻辑上的概念，patition是物理概念
     */
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // Use this in most cases, it will consistently distribute partitions across all executors.
      ConsumerStrategies.Subscribe[String, String]( // <= 注意泛型 K type of Kafka message key, type of Kafka message value
        Set("first"), // topics: Iterable[String] 主题
        kafkaParameters) // kafkaParams: collection.Map[String, Object] 其他配置
    )

    kafkaDStream.map(_.value())
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}
