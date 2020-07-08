package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * <p>KafkaSelectionInterceptor</p>
 *
 * <p>自定义 flume 的拦截器, 使按照 body 内容的 hashcode 取余来选择 channel</p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/19 2:39 下午
 */
public class KafkaSelectionInterceptor implements Interceptor {
    /**
     * Any initialization / startup needed by the Interceptor.
     */
    @Override
    public void initialize() {

    }

    /**
     * Interception of a single {@link Event}.
     *
     * @param event Event to be intercepted
     * @return Original or modified event, or {@code null} if the Event
     * is to be dropped (i.e. filtered out).
     */
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());
        int i = body.hashCode() % 3;
        switch(i){
            case 0:
                headers.put("topic", "first");
                break;
            case 1:
                headers.put("topic", "second");
                break;
            default:
                break;
        }
        return event;
    }

    /**
     * Interception of a batch of {@linkplain Event events}.
     *
     * @param events Input list of events
     * @return Output list of events. The size of output list MUST NOT BE GREATER
     * than the size of the input list (i.e. transformation and removal ONLY).
     * Also, this method MUST NOT return {@code null}. If all events are dropped,
     * then an empty List is returned.
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    /**
     * Perform any closing / shutdown needed by the Interceptor.
     */
    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new KafkaSelectionInterceptor();
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
