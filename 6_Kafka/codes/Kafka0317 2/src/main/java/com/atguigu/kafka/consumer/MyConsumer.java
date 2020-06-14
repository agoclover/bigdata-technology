package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者
 */
public class MyConsumer {
    public static void main(String[] args) {
        //0. 创建配置对象
        Properties props = new Properties();
        // 指定kafka集群的位置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // 指定消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu-group1");

        // 指定是否自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 提交offset的间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");


        // offset的重置
        /**
         * 满足两种情况会重置offset:
         *   1. 当前的消费者组之前没有在kafka中消费过(新的组新的人)
         *   2. 当前想消费者组中的某个消费者的offset 在kafka中对应的数据已经被删除(默认超过7天会删除)
         *
         * 重置的方式:
         *   earliest: automatically reset the offset to the earliest offset(头)
         *   latest: automatically reset the offset to the latest offset(尾) 默认值
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest") ;

        //指定kv的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //1. 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //2. 订阅主题
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("atguigu");
        topics.add("first");
        topics.add("second");
        topics.add("third");
        consumer.subscribe(topics);

        //3. 消费
        while (true){
            ConsumerRecords<String, String>  records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic() + " -- " +
                        record.partition() + " -- " +
                        record.offset()+" -- " +
                        record.key() + " -- " +
                        record.value());
            }
            //提交offset
            //同步提交
//            consumer.commitSync();
//            System.out.println("=====同步提交======");
            //异步提交
             //System.out.println("====异步提交=====");
//           consumer.commitAsync();
//             consumer.commitAsync(new OffsetCommitCallback() {
//                 @Override
//                 public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                     if(exception != null ){
//                         System.out.println("提交失败");
//                     }else{
//                         System.out.println("offsets:" + offsets);
//                     }
//                 }
//             });
        }
    }
}
