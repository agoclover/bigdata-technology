package com.atguigu.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/6/13 8:21 上午
 */
public class MySink extends AbstractSink implements Configurable {
    private Logger logger = LoggerFactory.getLogger(MySink.class);

    /**
     * 对 Event 的处理.
     * Flume 内部会循环调用.
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        // 获取 channel
        Channel ch = getChannel();
        // 获取事务对象
        Transaction txn = ch.getTransaction();
        // 开启事务
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do
            // 从 channel 中 take 数据
            Event event;

            while (true){
                Thread.sleep(1000);
                event = ch.take();
                if (event != null) {
                    break;
                }
            }
            // Send the Event to the external repository.
            storeSomeData(event);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }

    private void storeSomeData(Event event) {
        // 将 event 通过 logger 的方式打印到控制台
        logger.info(new String(event.getBody()));
    }

    /**
     * 用于读取配置文件
     * @param context
     */
    @Override
    public void configure(Context context) {
    }
}
