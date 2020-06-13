package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * <p>自定义 Interceptor - LogInterceptor</p>
 *
 * <p>IF event contains "atguigu" then send it to a channel,
 * else send it to another channel.
 * Using Multiplexing Channel Selector.
 * But this is used as interceptors.</p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/12 5:49 下午
 */
public class LogInterceptor implements Interceptor {
    /**
     * 初始化
     */
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        if (body.contains("atguigu")) {
            headers.put("title", "at");
        } else {
            headers.put("title", "ot");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    /**
     * 关闭一些流
     */
    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{
        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        /**
         * <p>
         * Request the implementing class to (re)configure itself.
         * </p>
         * <p>
         * When configuration parameters are changed, they must be
         * reflected by the component asap.
         * </p>
         * <p>
         * There are no thread safety guarantees on when configure might be called.
         * </p>
         *
         * @param context
         */
        @Override
        public void configure(Context context) {

        }
    }
}
