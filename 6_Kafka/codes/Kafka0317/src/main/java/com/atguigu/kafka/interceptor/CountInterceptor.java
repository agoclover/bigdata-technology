package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 *
 * 功能: 统计消息发送成功及失败的数量
 */
public class CountInterceptor implements ProducerInterceptor<String,String> {
    private Integer success = 0;
    private Integer fail = 0 ;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception == null ){
            //发送成功
            success ++ ;
        }else{
            fail ++ ;
        }
    }

    @Override
    public void close() {
        //将统计的结果打印到控制台
        System.out.println("SUCCESS: " + success );
        System.out.println("FAIL: " + fail);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
