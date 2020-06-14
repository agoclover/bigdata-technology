package com.atguigu.kafka.flumeInterceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import javax.swing.text.html.HTMLEditorKit;
import java.util.List;
import java.util.Map;

public class FlumeKafkaInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 如果包含"atguigu"的数据，发送到first主题
     * 如果包含"sgg"的数据，发送到second主题
     * 其他的数据发送到third主题
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //1.获取event的header
        Map<String, String> headers = event.getHeaders();
        //2.获取event的body
        String body = new String(event.getBody());
        if(body.contains("atguigu")){
            headers.put("topic","first");
        }else if(body.contains("sgg")){
            headers.put("topic","second");
        }
        return event;

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
          intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements  Builder{

        @Override
        public Interceptor build() {
            return  new FlumeKafkaInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
