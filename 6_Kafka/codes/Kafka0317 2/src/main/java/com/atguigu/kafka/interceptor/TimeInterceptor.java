package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 *
 * 需要实现ProducerInterceptor接口
 *
 *
 * 功能: 在每条消息的前面加上时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    /**
     * 拦截处理过程
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //1. 获取当前消息的value
        String value = record.value();
        //2. 将时间戳拼接到value的前面
        String resultValue = System.currentTimeMillis() + " -- " +  value;
        //3. 重新创建一个ProducerRecord对象
        ProducerRecord<String, String> newRecord =
                new ProducerRecord<>(record.topic(), record.partition(), record.key(), resultValue);

        return newRecord;
    }

    /**
     * 消息发送成功或者失败后会调用该方法。
     * @param metadata
     * @param exception
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 关闭资源等
     */
    @Override
    public void close() {

    }

    /**
     * 读取配置
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }
}
