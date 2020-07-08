package com.atguigu.zookeeper.utils;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/31 3:20 下午
 */
public class ZKUtils {

    public static ZooKeeper initZK(String connectionString, int sessionTimeout, Watcher watcher) throws IOException {
        ZooKeeper zk;
        if (watcher != null) {
            zk = new ZooKeeper(connectionString,
                    sessionTimeout,
                    watcher);
        } else {
            zk = new ZooKeeper(connectionString,
                sessionTimeout,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                    }
                });
//            zk = new ZooKeeper(connectionString, sessionTimeout, (e) -> {}); // lambda 表达式
        }
        return zk;
    }

    public static ZooKeeper initZK(String connectionString, int sessionTimeout) throws IOException {
        return initZK(connectionString, sessionTimeout, null);
    }
}
