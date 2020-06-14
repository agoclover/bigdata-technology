package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {
        //0. 创建配置对象
        Properties props = new Properties();
        //指定kafka的集群位置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //指定ack的级别
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        //指定重试次数
        props.put(ProducerConfig.RETRIES_CONFIG,5);
        //指定batch的大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //指定等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //指定RecordAccumulator缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //指定key的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //指定value的序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //指定拦截器
        ArrayList<String> interceptors = new ArrayList<>() ;
        interceptors.add("com.atguigu.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors) ;
        
        //1. 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //2. 生产数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String,String>("second","atguigu^^^^^" + i ));
        }
        //3. 关闭
        producer.close();
    }
}
