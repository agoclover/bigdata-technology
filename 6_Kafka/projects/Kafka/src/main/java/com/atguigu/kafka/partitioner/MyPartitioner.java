package com.atguigu.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * <p>自定义分区器</p>
 *
 * <p>自定义实现按照 value 分区, 包含 atguigu 进入 0 分区, 不包含 atguigu 进入 1 分区.</p>
 *
 * @author Zhang Chao
 * @version kafka_day3
 * @date 2020/6/18 8:32 上午
 */
public class MyPartitioner implements Partitioner {

    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (value.toString().contains("atguigu")) {
            return 0;
        } else {
            return 1;
        }
    }

    /**
     * This is called when partitioner is closed.
     * 关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * Configure this class with the given key-value pairs
     * 读取配置文件
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
