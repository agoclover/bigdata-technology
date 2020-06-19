package com.atguigu.kafka.comsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        /*
        自动提交是按照一定时间间隔提交 offset 的.
        一个消费者组, 第一次启动里面没有 offset, 此时会触发 ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 默认为
        latest, 即打开后不消费以前的数据, 但能看到新进来的数据.

        当消费数据后, 就会产生新的消费者组的 offset 数据, 表示这个消费者组对某个 topic 消费到了哪里, 当 consumer
        进程存在时, 此时这个 offset 存在内存里. 这个时候就应该提交 offset, 也就是自动提交, 这样就持久化到 kafka 的
        记录 offset 的那个 topic 中了 (磁盘内). 但若 ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG = false,
        那么 offset 就无法提交, 但此时内存中还记录着 offset 信息, 则新生产数据后还是会正常只消费新数据.

        但若此时停掉 consumer 进程, 内存中的 offset 就消失了, 也没有持久化到 kafka 中, kafka 对此消费者组还保留的
        是上一次打开时最后消费的数据 offset, 所以再次打开这个消费者组就会按照这个 offset 开始消费.

        这个时候可以开启 consumer.commitSync()/consumer.commitAsync() 同步/异步提交, 那么就会记录消费的 offset 了.
         */
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

        Type: string Default: latest Valid Values: [latest, earliest, none]Importance: medium

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

            // 开启同步提交: 消费一个提交一个
            consumer.commitSync();
            // 开启异步提交: 一边消费, 一边提交
//            consumer.commitAsync();
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println("Error");
                    } else {
                        System.out.println(offsets);
                    }
                }
            });
        }
    }
}
