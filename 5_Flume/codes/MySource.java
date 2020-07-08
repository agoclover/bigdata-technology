package com.atguigu.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.UUID;

/**
 * 自定义Source需要继承AbstractSource ，实现 Configurable 、 PollableSource 接口
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String  prefix;
    /**
     * flume内部会循环调用该方法，进行日志的采集
     * 主要用于日志的采集
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            // 采集日志并包装成Event对象
            Event e = getEvent();
            // 获取ChannelProcessor
            ChannelProcessor processor = getChannelProcessor();
            // 通过ChannelProcessor处理event
            processor.processEvent(e);
            status = Status.READY;
        } catch (Throwable t) {

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }
        return status;
    }

    /**
     * 采集日志并封装event的核心方法
     * @return
     */
    private Event getEvent() throws InterruptedException {
        //一秒钟一条
        Thread.sleep(1000);
        //模拟日志数据
        String log = UUID.randomUUID().toString();
        //封装Event
        SimpleEvent event = new SimpleEvent();
        //设置header
        event.getHeaders().put("myLog","UUID");
        //设置body
        event.setBody((prefix +log).getBytes());

        return event;
    }

    /**
     * 每次backoff的增长时间
     * 进小黑屋以后，每次增长多长时间
     * @return
     */
    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    /**
     * backoff的最长时间
     * @return
     */
    @Override
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    /**
     * 用于读取配置文件
     * @param context
     */
    @Override
    public void configure(Context context) {
        // 获取flume配置文件中的配置项
        prefix = context.getString("prefix","logs-");
    }
}
