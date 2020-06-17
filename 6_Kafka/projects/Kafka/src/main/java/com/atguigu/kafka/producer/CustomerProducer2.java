package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

/**
 * <p>CustomerProducer2 </p>
 *
 * <p>自定义 Kafka 生产者
 * 使用 Kafka 提供的配置类:
 * 1. ProducerConfig   生产者相关的配置信息
 * 2. CommonClientConfigs  生产者和消费者通用的配置信息
 * 3. ConsumerConfig   消费者相关的配置信息</p>
 *
 * @author Zhang Chao
 * @version Kafka_day2
 * @date 2020/6/17 9:36 上午
 */
public class CustomerProducer2 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord("first", i + "--> " + UUID.randomUUID().toString()));
        }

        producer.close();
    }
}
