package com.atguigu.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 *
 * 需要实现Partitioner接口
 */
public class MyPartitioner  implements Partitioner {

    /**
     * 计算当前消息的分区号
     * @param topic  主题
     * @param key    消息的key
     * @param keyBytes  序列化后的key
     * @param value  消息的value
     * @param valueBytes 序列化后的value
     * @param cluster
     * @return
     *
     *
     * 模拟实现:
     *    使用second主题, 有两个分区
     *    消息带有atguigu的，去往0号分区
     *    其他的消息去往1号分区
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if(value.toString().contains("atguigu")){
            return  0 ;
        }else{
            return  1 ;
        }
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * 读取配置的
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
