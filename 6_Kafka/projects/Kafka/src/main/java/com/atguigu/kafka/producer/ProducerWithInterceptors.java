package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * <p>ProducerWithInterceptors</p>
 *
 * <p>自定义生产者, 并配置两个自定义的生产者拦截器.</p>
 *
 * @author Zhang Chao
 * @version kafka_day3
 * @date 2020/6/19 2:17 下午
 */
public class ProducerWithInterceptors {
    public static void main(String[] args) {
        // 配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器集合
        List<String> list = new ArrayList<>();
        list.add("com.atguigu.kafka.interceptor.PrefixInterceptor");
        list.add("com.atguigu.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        //
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            // 粘性分区: the sticky partition that changes when the batch is full
            producer.send(new ProducerRecord("first", i + " ==> Interceptor ==> " + UUID.randomUUID().toString()));
        }

        producer.close();
    }
}
