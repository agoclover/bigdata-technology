package com.atguigu.spark

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/18 2:32 下午
 */
object D1_Top3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.sparkContext.setCheckpointDir("/Users/amos/BigdataLearn/10_Spark/projects/Spark-Realtime/cp_d1")

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

    val userAdAction: DStream[((Long, String, Int), Int)] = resRDD.map(str => {
      val fields: Array[String] = str.split(",")
//      println(fields.mkString("-"))
      val action = UserAdAction(fields(0).toLong,
        fields(1),
        fields(2),
        fields(3).toInt,
        fields(4).toInt)
      ((action.day, action.area, action.ad_id), 1)
    }
    )
    val sums: DStream[((Long, String, Int), Int)] = userAdAction.updateStateByKey(
      (seq: Seq[Int], state: Option[Int]) => {
        Option(seq.sum + state.getOrElse(0))
      }
    )

    sums.map {
      case ((day, area, ad), counts) => ((day, area, counts), ad)
    }.foreachRDD(RDD =>
      RDD.sortByKey(false)
    .map{case ((day, area, ad), counts) => (day, area, ad, counts)}
        .collect()
        .foreach(println)
    )

    //打印输出

    ssc.start()
    ssc.awaitTermination()
  }
}
