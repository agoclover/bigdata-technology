package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * <p>CustomerComsumer1</p>
 *
 * <p>自定义 kafka 生产者
 * 自己配置配置项</p>
 *
 * @author Zhang Chao
 * @version Kafka_day2
 * @date 2020/6/17 9:17 上午
 */
public class CustomerProducer1 {
    public static void main(String[] args) {

        Properties props = new Properties();

        // kafka集群，broker-list
        props.put("bootstrap.servers", "hadoop102:9092");

        // ack 应答级别
        props.put("acks", "all");

        // 重试次数
        props.put("retries", 1);

        // 批次大小 16 M
        props.put("batch.size", 16384);

        // 等待时间
        props.put("linger.ms", 1);

        // RecordAccumulator缓冲区大小 32 M
        props.put("buffer.memory", 33554432);

        // 序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("first", i + " ==> " + UUID.randomUUID().toString()));
        }

        producer.close();
    }
}
