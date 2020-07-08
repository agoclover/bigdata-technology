package com.atguigu.zookeeper.task;

import com.atguigu.zookeeper.utils.ZKUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * <p>服务器</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version zookeeper_day2
 * @date 2020/5/31 3:33 下午
 */
public class Server {
    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 1000;
    private ZooKeeper zk;
    private String serverNode = "/servers";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Server server = new Server();
        //1. 初始化客户端对象
        server.init();
        //2. 判断存放服务器列表的znode是否存在
        server.initServerNode();
        //3. 在存放服务器列编排的Znode下创建当前服务器的临时节点
        server.registCurrentNode(args[0], args[1]);
        //4. 服务器保持在线
        Thread.sleep(Long.MAX_VALUE);
    }

    private void registCurrentNode(String serverName, String data) throws KeeperException, InterruptedException {
        String currentServerNode =
                zk.create(serverNode + "/" + serverName, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(currentServerNode + "IS NOW ONLINE!");
    }

    private void initServerNode() throws KeeperException, InterruptedException {
        Stat stat = zk.exists(serverNode, false);
        if (stat == null) {
            zk.create(serverNode, "servers".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private void init() throws IOException {
        zk = ZKUtils.initZK(connectionString, sessionTimeout);
    }
}
