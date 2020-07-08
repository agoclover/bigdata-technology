package com.atguigu.zookeeper.task;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * <p>客户端</p>
 *
 * <p>监控服务器状态</p>
 *
 * @author Zhang Chao
 * @version zookeeper_day2
 * @date 2020/5/31 4:05 下午
 */
public class Client {
    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 1000;
    private ZooKeeper zk;
    private String serverNode = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 这里假设 /servers 已经存在
        Client client = new Client();
        //1. 初始化客户端对象
        client.init();
        //2. 获取当前在线服务器列表, 并循环监控
        client.getOnlineServer();
        //3. 保持客户端在线
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getOnlineServer() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(serverNode, e -> {
            try {
                getOnlineServer();
            } catch (KeeperException keeperException) {
                keeperException.printStackTrace();
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        });
        System.out.println("Online Servers: " + children);
    }

    private void init() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, (e) -> {});
    }

}
