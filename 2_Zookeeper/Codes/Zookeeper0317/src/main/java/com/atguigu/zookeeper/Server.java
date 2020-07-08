package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 表示服务器，服务器上线会在Zk中创建对应的临时节点，服务器下线，临时节点被删除
 */
public class Server {
    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 10000 ;
    private ZooKeeper zk ;
    private String serverNode = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Server server = new Server() ;
        //1. 初始化Zk的客户端对象
        server.initZk();
        //2. 判断存放服务器列表的Znode是否存在
        server.initServerNode();
        //3. 在存放服务器列表的Znode中创建当前Server对应的临时节点
        server.registCurrentServer(args[0],args[1]);
        //4. 保持服务器的在线.
        Thread.sleep(Long.MAX_VALUE);
    }

    private void registCurrentServer(String serverName, String data) throws KeeperException, InterruptedException {
        String currentServerNode =
                zk.create(serverNode + "/" + serverName, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(currentServerNode +" Is On Line!!!");
    }

    private void initServerNode() throws KeeperException, InterruptedException {
        Stat stat = zk.exists(serverNode, false);
        if(stat == null ){
            //创建该节点
            zk.create(serverNode,"servers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }
    private void initZk() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, (e)->{});
    }
}
