package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * 生产者
 */
public class MyProducer {

    public static void main(String[] args) {  // main线程
        //0. 创建配置对象
        Properties props= new Properties();
        // 指定kafka的集群位置
        props.put("bootstrap.servers", "hadoop102:9092");
        // 指定ack的级别 ， 默认是1
        props.put("acks", "all");
        //重试次数
        props.put("retries", 5);
        //批次大小
        props.put("batch.size", 16384); // 16kb
        //等待时间
        props.put("linger.ms", 0);

        //RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432);  //32M

        // key的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value的序列化器
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //1. 创建生产者对象
        KafkaProducer producer = new KafkaProducer<String,String>(props);

        //2. 生产数据
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("second", i+"-->"+UUID.randomUUID().toString()));
        }
        //3. 关闭生产者对象
        producer.close();
    }
}
