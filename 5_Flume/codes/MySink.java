package com.atguigu.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义sink需要继承AbstractSink ，实现Configurable接口
 */
public class MySink  extends AbstractSink implements Configurable {

    Logger logger = LoggerFactory.getLogger(MySink.class);

    /**
     * flume内部会循环调用,对Event的处理
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // 获取Channel
        Channel ch = getChannel();
        // 获取事务
        Transaction txn = ch.getTransaction();
        //开启事务
        txn.begin();
        try {
            Event event ;
            // 从channel中take  event
            while(true){
                event = ch.take() ;
                if(event == null) {
                    Thread.sleep(1000);
                    continue ;
                }
                break;
            }
            // 对event的处理
            storeSomeData(event);
            // 提交事务
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            //回滚
            txn.rollback();
            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally{
            //关闭事务
            txn.close();
        }
        return status;
    }

    /**
     * event的处理过程
     * @param event
     */
    private void storeSomeData(Event event) {
        // 将event通过logger的方式打印到控制台
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
