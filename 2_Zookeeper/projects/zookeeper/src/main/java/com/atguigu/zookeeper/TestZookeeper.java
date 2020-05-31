package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * <p>zookeeper API test</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version zookeeper_day1
 * @date 2020/5/30 3:14 下午
 */
public class TestZookeeper {
    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 10000; // 4000 ~ 40000
    ZooKeeper zk;

    /**
     * 删除所有节点
     */
    @Test
    public void testDeleteAll() throws KeeperException, InterruptedException {
        deleteAll("/atguigu");
    }

    /**
     * 删除节点
     */
    @Test
    public void testDelete() throws KeeperException, InterruptedException {
//        zk.delete("/atguigu/dan", -1);
        String path = "/atguigu/dan";
        Stat stat = getStat(path);
        // 不能删除有子节点的目录
        zk.delete(path, stat.getVersion());
    }

    /**
     * 删除所有节点
     * @param path 被删除的路径
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteAll(String path) throws KeeperException, InterruptedException {
        Stat stat = getStat(path);
        if (stat != null) {
            List<String> children = zk.getChildren(path, false);
            if (children.size() > 0) {
                for (String child : children) {
                    deleteAll(path + "/" + child);
                }
                deleteAll(path);
            } else {
                zk.delete(path, stat.getVersion());
            }
        }
    }

    /**
     * 修改节点的内容
     */
    @Test
    public void testSet() throws KeeperException, InterruptedException {
        String path = "/atguigu/dan";
        Stat stat = getStat(path);
        int version = stat.getVersion();
//        Stat stat1 = zk.setData(path, "cute".getBytes(), version );
        Stat stat1 = zk.setData(path, "adorable".getBytes(), -1);
        System.out.println(stat);

        System.out.println(stat1);
    }

    /**
     * 创建节点
     */
    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        String s =
                zk.create("/atguigu/dan",
                        "cute".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
        System.out.println("s= " + s);
    }

    /**
     * 获取节点的内容
     */
    @Test
    public void testGet() throws KeeperException, InterruptedException {
        String path = "/atguigu";
        Stat stat = getStat(path);
        if (stat != null) {
            byte[] data = zk.getData(path, false, stat);
            System.out.println(new String(data));
        }
    }

    public Stat getStat(String path) throws KeeperException, InterruptedException {
        return zk.exists(path, false);
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat stat = zk.exists("/atguigu", false);
        if (stat != null) {
            System.out.println("/atguigu exists.");
        } else {
            System.out.println("/atguigu dose not exists.");
        }
    }

    /**
     * 获取子节点
     */
    @Test
    public void testLS() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/atguigu", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("监听的节点子目录发生了变化");
            }
        });

        for (String child : children) {
            System.out.println("child = " + child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }


    @Before
    public void init() throws IOException {
        zk = new ZooKeeper(connectionString,
                sessionTimeout,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                    }
                });
    }

    @After
    public void close(){
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testZKClient() throws IOException {
        zk = new ZooKeeper(connectionString,
                sessionTimeout,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        // 当监听的事件发生后, 会执行该方法进行事件的处理

                        // 例如监听某个Znode子节点的变化, 当被监听的Znode子节点变化后, 会执行process方法
                        // 我们可以在 process 方法中写出要做的事情
                    }
                });

        System.out.println("zkClient = " + zk);

        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
