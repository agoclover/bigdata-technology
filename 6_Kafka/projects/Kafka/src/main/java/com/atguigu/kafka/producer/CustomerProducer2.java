package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 制定分区器
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.kafka.partitioner.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            // 粘性分区: the sticky partition that changes when the batch is full
//            producer.send(new ProducerRecord("first", i + "--> " + UUID.randomUUID().toString()));

            // 按照指定的 key 的 hash 值对分区数取余得到分区号
//            producer.send(new ProducerRecord("first", "key" + i, i + " ==> " + UUID.randomUUID().toString()));

            // 制定分区号
//            producer.send(new ProducerRecord("first", i%2, "key", i + " ==> " + UUID.randomUUID().toString()));

            // 自定义分区测试
//            if ((i % 2) == 0) {
//                producer.send(new ProducerRecord("first", i + "==> atguigu"));
//            } else {
//                producer.send(new ProducerRecord("first", i + "==> shangguigu"));
//            }

            // 带回调函数消息, 异步发送测试
//            producer.send(new ProducerRecord("first", i + "--> " + UUID.randomUUID().toString()),
//                    new Callback() {
//                        /**
//                         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
//                         * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
//                         * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
//                         *
//                         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
//                         *                  with -1 value for all fields except for topicPartition will be returned if an error occurred.
//                         * @param exception The exception thrown during processing of this record. Null if no error occurred.
//                         *                  Possible thrown exceptions include:
//                         *                  <p>
//                         *                  Non-Retriable exceptions (fatal, the message will never be sent):
//                         *                  <p>
//                         *                  InvalidTopicException
//                         *                  OffsetMetadataTooLargeException
//                         *                  RecordBatchTooLargeException
//                         *                  RecordTooLargeException
//                         *                  UnknownServerException
//                         *                  <p>
//                         *                  Retriable exceptions (transient, may be covered by increasing #.retries):
//                         *                  <p>
//                         *                  CorruptRecordException
//                         *                  InvalidMetadataException
//                         *                  NotEnoughReplicasAfterAppendException
//                         *                  NotEnoughReplicasException
//                         *                  OffsetOutOfRangeException
//                         *                  TimeoutException
//                         */
//                        @Override
//                        public void onCompletion(RecordMetadata metadata, Exception exception) {
//                            if (exception != null) {
//                                System.out.println("Data failure. " + metadata.offset());
//                            } else {
//                                System.out.println(metadata.topic() + " -- " +
//                                        metadata.partition() + " -- " +
//                                        metadata.offset());
//                            }
//                        }
//                    });

            // 带回调函数消息, 同步发送测试
            Future future = producer.send(new ProducerRecord("first", i + "--> " + UUID.randomUUID().toString()),
                    new Callback() {
                        /**
                         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
                         * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
                         * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
                         *
                         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
                         *                  with -1 value for all fields except for topicPartition will be returned if an error occurred.
                         * @param exception The exception thrown during processing of this record. Null if no error occurred.
                         *                  Possible thrown exceptions include:
                         *                  <p>
                         *                  Non-Retriable exceptions (fatal, the message will never be sent):
                         *                  <p>
                         *                  InvalidTopicException
                         *                  OffsetMetadataTooLargeException
                         *                  RecordBatchTooLargeException
                         *                  RecordTooLargeException
                         *                  UnknownServerException
                         *                  <p>
                         *                  Retriable exceptions (transient, may be covered by increasing #.retries):
                         *                  <p>
                         *                  CorruptRecordException
                         *                  InvalidMetadataException
                         *                  NotEnoughReplicasAfterAppendException
                         *                  NotEnoughReplicasException
                         *                  OffsetOutOfRangeException
                         *                  TimeoutException
                         */
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                System.out.println("Data failure. " + metadata.offset());
                            } else {
                                System.out.println(metadata.topic() + " -- " +
                                        metadata.partition() + " -- " +
                                        metadata.offset());
                            }
                        }
                    });
            System.out.println("=== 消息发送完成 ===");
            /*
            阻塞线程, 直到 get() 方法返回结果.
            这是一个 JUC, 即 java.util.concurrent 多线程包. 需要看看.
            结果打印(使用默认的粘性分区):
            first -- 0 -- 449
            === 消息发送完成 ===
            first -- 1 -- 859
            === 消息发送完成 ===
            first -- 0 -- 450
            === 消息发送完成 ===
            first -- 1 -- 860
            === 消息发送完成 ===
            first -- 0 -- 451
            === 消息发送完成 ===
            first -- 1 -- 861
            === 消息发送完成 ===
            first -- 0 -- 452

            可以发现, 分区在轮循, 这是因为等待时间超过 linger ms.
            如果是异步发送, 结果打印:

            === 消息发送完成 ===
            === 消息发送完成 ===
            === 消息发送完成 ===
            === 消息发送完成 ===
            [INFO] [2020-06-19 07:40:33][org.apache.kafka.clients.producer.KafkaProducer][Producer clientId=producer-1] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
            first -- 0 -- 453
            first -- 0 -- 454
            first -- 0 -- 455
            first -- 0 -- 456
             */
            Object o = future.get();

        }

        // 等待 1 second
//        Thread.sleep(1000);
//        TimeUnit.MILLISECONDS.sleep(1000);
        TimeUnit.SECONDS.sleep(1);

//        producer.close();
        /*
        producer.close() 这个函数一个作用就是先确保消息发送, 再关闭相应生产者. 所以加上 close() 一方面会让我们的生产者关闭, 但另一方面还会
        保证消息的发出.
        如果不加 close() 方法, 消息的发送依赖以下两个参数:
            BATCH_SIZE_CONFIG, 16384
            LINGER_MS_CONFIG, 1
        即或消息量达到 16kb, 或等待时间超过 1ms, 很明显我们以上的数据到不了 16kb, 同时就在等待线程拉取数据的 1ms 中, main 线程其实已经结束
        了. 所以如果不加 close() 则会发不出消息. 但可以通过使线程等待若干时间来保证有线程可以将数据拉取发送.
         */
    }
}
