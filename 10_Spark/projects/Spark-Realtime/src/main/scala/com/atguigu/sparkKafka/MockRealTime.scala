package com.atguigu.sparkKafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

object MockRealTime {
    /*
  数据格式:
  timestamp area city userid adid
  某个时间点 某个地区 某个城市 某个用户 某个广告
  
  */
    def mockRealTimeData(): ArrayBuffer[String] = {
        // 存储模拟的实时数据
        val array = ArrayBuffer[String]()
        // 城市信息
        val randomOpts = RandomOptions(
            (CityInfo(1, "北京", "华北"), 30),
            (CityInfo(2, "上海", "华东"), 30),
            (CityInfo(3, "广州", "华南"), 10),
            (CityInfo(4, "深圳", "华南"), 20),
            (CityInfo(4, "杭州", "华中"), 10))
        (1 to 50).foreach {
            i => {
                val timestamp = System.currentTimeMillis()
                val cityInfo = randomOpts.getRandomOption()
                val area = cityInfo.area
                val city = cityInfo.city_name
                val userid = RandomNumUtil.randomInt(100, 105)
                val adid = RandomNumUtil.randomInt(1, 5)
                array += s"$timestamp,$area,$city,$userid,$adid"
                Thread.sleep(10)
            }
        }
        array
    }
    
    def createKafkaProducer: KafkaProducer[String, String] = {
        val props: Properties = new Properties
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        new KafkaProducer[String, String](props)
    }
    
    def main(args: Array[String]): Unit = {
        val topic = "ad_log_0317"
        val producer: KafkaProducer[String, String] = createKafkaProducer
        while (true) {
            mockRealTimeData().foreach {
                msg => {
                    producer.send(new ProducerRecord(topic, msg))
                    println("msg = " + msg);
                    Thread.sleep(100)
                }
            }
            Thread.sleep(1000)
        }
    }
}
