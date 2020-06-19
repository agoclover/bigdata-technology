package com.atguigu.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.UUID;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/13 7:49 上午
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;

    /**
     * 循环调用, 用于采集.
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        try {
            // This try clause includes whatever Channel/Event operations you want to do

            // Receive new data
            Event e = getSomeData();

            // Store the Event into this Source's associated Channel(s)
            getChannelProcessor().processEvent(e);

            status = Status.READY;
        } catch (Throwable t) {
            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }
        return status;
    }

    /**
     * 自定义 source 数据.
     * @return
     */
    private Event getSomeData() throws InterruptedException {
        // 模拟日志数据
        Thread.sleep(1000);
        UUID uuid = UUID.randomUUID();
        String log = uuid.toString();
        // 封装 Event
        SimpleEvent event = new SimpleEvent();
        // 设置 header
//        event.getHeaders().put("id", "uuid");
        // 设置 body
        event.setBody((prefix + log).getBytes());

        return event;
    }

    /**
     * 每次 backoff 增长的时间, 进入 blacklist 以后每次增长的时间.
     * @return
     */
    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    /**
     * 最大时间
     * @return
     */
    @Override
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    /**
     * 读取配置
     * @param context
     */
    @Override
    public void configure(Context context) {
        // 获取 flume 配置文件 .conf 中的配置项
        prefix = context.getString("prefix", "dpre--");

    }
}
