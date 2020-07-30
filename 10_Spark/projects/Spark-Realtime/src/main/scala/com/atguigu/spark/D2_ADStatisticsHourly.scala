package com.atguigu.spark

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/7/18 3:19 下午
 */
object D2_ADStatisticsHourly {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(6))
    ssc.sparkContext.setCheckpointDir("/Users/amos/BigdataLearn/10_Spark/projects/Spark-Realtime/cp_d2")

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

    val adPerMin: DStream[((Long, Int), Int)] = resRDD.map(str => {
      val fields: Array[String] = str.split(",")
      //      println(fields.mkString("-"))
      val action = UserAdAction(fields(0).toLong,
        fields(1),
        fields(2),
        fields(3).toInt,
        fields(4).toInt)
      ((action.min, action.ad_id), 1)
    }
    )

    val windowDS: DStream[((Long, Int), Int)] = adPerMin.window(Minutes(60), Seconds(6))

    windowDS.reduceByKey(_+_)
      .foreachRDD(RDD=>{
        RDD.sortBy{case ((min, ad), counts)=>(min, -counts)}
          .collect()
          .foreach(println)
      })

    ssc.start()

    ssc.awaitTermination()

  }
}
