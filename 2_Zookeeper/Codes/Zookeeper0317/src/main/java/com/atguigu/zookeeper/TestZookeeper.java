package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 客户端的操作:
 *   1. 先创建客户端对象
 *   2. 调用API方法
 *   3. 关闭资源
 */
public class TestZookeeper {

    private String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeOut = 10000 ;   // 4000(最小超时时间) ~ 40000(最大超时时间)
    private ZooKeeper zk ;


    /**
     * 删除非空的Znode
     */
    @Test
    public void testDeleteNodes() throws KeeperException, InterruptedException {
        deleteNodes("/a",zk);
    }

    public void deleteNodes( String path ,ZooKeeper zk) throws KeeperException, InterruptedException {
        //判断是否有孩子
        List<String> children = zk.getChildren(path, false);
        if(children!=null && children.size() ==0){
            //没有孩子,直接删除
            zk.delete(path,-1);
        }else{
            //有孩子,将每个孩子节点传入当前方法，进行删除
            //   /a   /a/b  /a/d  /a/b/c  /a/d/e
            for (String child : children) {
                //删除孩子
                deleteNodes(path +"/" +child,zk);  //  /a/b   /a/d
            }
            //删除当前的节点
            deleteNodes(path,zk);
        }
    }


    /**
     * 删除
     */
    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        // 删除空Znode
        zk.delete("/atguigu/def",-1);

        //如何删除非空的Znode??
    }


    /**
     * 设置节点的内容
     */
    @Test
    public void testSet() throws KeeperException, InterruptedException {
        Stat stat = getStat("/atguigu");
        //Stat resultStat  = zk.setData("/atguigu", "shangguigu".getBytes(), stat.getVersion());
         Stat resultStat  = zk.setData("/atguigu", "sgg".getBytes(), -1);
    }


    /**
     * 创建节点
     */
    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        String s =
                zk.create("/atguigu/fangfang",
                         "fast".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("s = " + s);

    }


    /**
     * 获取节点的内容
     */
    @Test
    public void testGet() throws KeeperException, InterruptedException {
        //zk.getData(path,true/false,stat);
        //zk.getData(path,watcher,stat)
        Stat stat = getStat("/atguigu");
        if(stat  == null ){
            System.out.println("/atguigu 不存在");
            return ;
        }
        byte[] data = zk.getData("/atguigu", false, stat);
        System.out.println("data = " +  new String(data));
    }

    public  Stat  getStat(String path) throws KeeperException, InterruptedException {

        return zk.exists(path ,false) ;
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void testExists() throws KeeperException, InterruptedException {
        Stat stat = zk.exists("/atguigu", false);
        if(stat == null ){
            System.out.println("/atguigu 不存在");
        }else{
            System.out.println("stat:" + stat );
            System.out.println("/atguigu 存在");
        }
    }

    /**
     * 获取子节点
     */
    @Test
    public void testLs() throws KeeperException, InterruptedException {
        //  zk.getChildren(path ,watcher) ;  获取path的子节点，并通过指定的watcher进行监听。
        //  zk.getChildren(path ,false/true) 获取path的子节点，如果传的false，则不监听，如果传的true，则使用默认的watcher进行监听。

        //List<String> children = zk.getChildren("/atguigu", false);
        List<String> children = zk.getChildren("/atguigu", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //重新读取/atguigu的子节点
                System.out.println("监听到/atguigu 节点的子节点发生变化");
            }
        });
        for (String child : children) {
            System.out.println("child = " + child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }





    @Before
    public void init() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    @After
    public void close() throws InterruptedException {
        zk.close();
    }


    @Test
    public  void testZkClient() throws IOException, InterruptedException {
        String connectionString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeOut = 10000 ;   // 4000(最小超时时间) ~ 40000(最大超时时间)
        ZooKeeper zkClient = new ZooKeeper(connectionString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //当监听的事件发生后，会执行该方法进行事件的处理.

                //例如监听某个Znode子节点的变化， 当被监听的Znode的子节点发生变化后，会指定process方法,
                //我们可以在process方法中写出要做的事情.
            }
        });

        System.out.println("zkClient = " + zkClient);
        //关闭
        zkClient.close();
    }
}



















