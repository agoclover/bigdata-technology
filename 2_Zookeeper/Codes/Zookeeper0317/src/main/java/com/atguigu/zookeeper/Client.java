package com.atguigu.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * 从zk中存放服务器列表的Znode中读取数据,并循环监听
 */
public class Client {
    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 10000 ;
    private ZooKeeper zk ;
    private String serverNode = "/servers";
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Client client  = new Client();
        //1. 初始化zk客户端对象
        client.initZk();
        //2. 获取当前在线的server列表,并循环监听
        client.getOnLineServer();
        //3. 保持客户端的在线
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getOnLineServer() throws KeeperException, InterruptedException {
        //没有考虑zk中  /servers 不存在的情况.
        List<String> children = zk.getChildren(serverNode, (e) -> {
            //监听到子节点发生变化， 重新获取子节点
            try {
                getOnLineServer();
            } catch (KeeperException ex) {
                ex.printStackTrace();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        });
        // 打印当前在线的服务列表
        System.out.println("Current On Line Server : "  + children);
    }

    private void initZk() throws IOException {
        zk = new ZooKeeper(connectionString,sessionTimeout,(e)->{});
    }
}
