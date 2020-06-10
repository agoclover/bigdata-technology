package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * 需求:
 *    将日志中带有'atguigu'的日志发送channel1
 *    其他的日志数据发送 channel2
 * 分析:
 *    需要使用Multiplexing Channel Selector, 会根据event中的header中的kv决定将日志给到哪个channel
 * 编码:
 *    自定义interceptor ，然后在event中按照body的数据进行判断处理, 在每个event的header中添加对应的kv
 *
 *
 */
public class LogInterceptor implements Interceptor {
    /**
     * 初始化方法
     */
    @Override
    public void initialize() {

    }

    /**
     * 拦截方法， 对单个event的拦截处理
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //1.获取event的header
        Map<String, String> headers = event.getHeaders();   // kv
        //2.获取event的body
        String body  = new String(event.getBody());
        //3.判断日志数据是否包含atguigu
        if(body.contains("atguigu")){
            headers.put("title","at");  //atguigu
        }else{
            headers.put("title","ot");  //other
        }
        // 疑问: 不需要把headers设置到event中吗？ 不需要
        // event.setHeaders(headers);
        return event;
    }

    /**
     * 拦截方法，对批次的event进行拦截处理
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list ;
    }
    /**
     * 如果有资源需要关闭，可在close方法中完成.
     */
    @Override
    public void close() {

    }


    public static class MyBuilder implements  Builder{

        /**
         * 用于创建Interceptor对象的
         * @return
         */
        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        /**
         * 用于读取flume的配置文件
         * @param context
         */
        @Override
        public void configure(Context context) {

        }
    }
}
