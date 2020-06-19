package com.atguigu.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <p>MyConsumer</p>
 *
 * <p>自定义消费者</p>
 *
 * @author Zhang Chao
 * @version kafka_day3
 * @date 2020/6/19 8:37 上午
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 指定 kafka 集群的位置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        // 指定消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        // 指定是否自动提交 offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 指定 offset 的间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 指定 kv 的反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 重置 offset
        /*
        auto.offset.reset: What to do when:
            1. there is no initial offset in Kafka (new consumer group)
            2. or if the current offset does not exist any more on the server
               e.g. because that data has been deleted
        earliest: automatically reset the offset to the earliest offset 这样一个新的消费者组就可以获得所有之前的消息.
        latest: automatically reset the offset to the latest offset
        none: throw exception to the consumer if no previous offset is found for the consumer's group
        anything else: throw exception to the consumer.

        Type: stringDefault: latestValid Values: [latest, earliest, none]Importance: medium

         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 1. 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 2. 订阅主题
        /*
        通过 zkCli.sh 的 ls /brokers/topics 查看有哪些主题
         */
        List<String> topics = new ArrayList<>();
        topics.add("first");
        topics.add("second"); // 不存在的主题也可以订阅
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                /*
                %d 整数型(10进制 )
                %f 浮点数(10进制)
                %e 浮点数(科学计数)
                %x 整数(16进制)
                %c Unicode字符
                %s String
                %b Boolean值
                %h 散列码(16进制)
                %% 字符"%"
                %n 通用换行, 即保证在 linux win 下均可换行
                 */
                System.out.printf("partition=%d, offset=%d, key=%s, value=%s\n",
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());
            }
        }
    }
}
