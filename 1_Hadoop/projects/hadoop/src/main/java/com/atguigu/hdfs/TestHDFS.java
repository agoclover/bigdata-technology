package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * 开发客户端类型的代码的套路:
 *
 * 1. 创建或者获取 客户端对象
 * 2. 调用API进行具体的操作
 * 3. 关闭
 *
 */
public class TestHDFS {
    private URI uri =URI.create("hdfs://hadoop102:9820");
    private Configuration conf = new Configuration() ;
    private String user = "atguigu";
    private FileSystem fs  ;


    /**
     * 文件详情查看
     */
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> its =
                                fs.listFiles(new Path("/"), true);
        //迭代
        while(its.hasNext()){
            //获取下一个
            LocatedFileStatus fileStatus =  its.next();
            System.out.println("============" + fileStatus.getPath().getName()+"============");
            System.out.println("文件路径: " + fileStatus.getPath());
            System.out.println("文件长度: " + fileStatus.getLen());
            System.out.println("文件权限: " + fileStatus.getPermission());
            System.out.println("文件副本: " + fileStatus.getReplication());
            System.out.println("文件块大小 : " + fileStatus.getBlockSize());
            System.out.println("文件所属主 : " + fileStatus.getOwner());
            System.out.println("文件所属组 : " + fileStatus.getGroup());
            System.out.println("文件块位置 : " + Arrays.toString(fileStatus.getBlockLocations()));
            System.out.println();
        }
    }

    /**
     * 文件的更名和移动
     *
     */
    @Test
    public void testRename() throws IOException {
        //改名
        //fs.rename(new Path("/java/longlong.txt"),new Path("/java/longge.txt"));
        //移动
        fs.rename(new Path("/java/longge.txt"),new Path("/longer.txt"));
    }

    /**
     * 文件删除
     */
    @Test
    public void testDelete()throws Exception{
       // boolean b = fs.delete(new Path("/bigdata/songsong.txt"),false);
        boolean b = fs.delete(new Path("/bigdata"),true);
    }


    /**
     * 文件下载
     *
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false,
                           new Path("/bigdata/yaner.txt"),
                           new Path("D:/hadoop"),
                           true);   //crc-32
    }


    /**
     * 参数的优先级
     *  Configuration对象设置的参数(代码) >  resources/xxx-site.xml(module) >  xxx-site.xml(集群)  >  xxx-default.xml(集群)
     *
     *  如果配置是通用的，一般不会变，建议直接在集群中设置。
     *  如果配置是临时使用，作为覆盖集群配置来用， 建议在module的xml文件或者是代码中的Configuration来进行设置.
     */

    /**
     * 文件上传
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(false,true,
                            // new Path("D:\\hadoop\\songsong.txt") ,
                            // new Path("D:/hadoop/songsong.txt"),
                             new Path("D:"+ File.separator+"hadoop"+File.separator+"longlong.txt"),
                             new Path("/java"));
    }




    /**
     * @Before注解标注的方法会自动在@Test标注的方法执行之前执行。
     *
     */
    @Before
    public void init() throws Exception{
        // 设置副本数
        // conf.set("dfs.replication","1");
        fs  = FileSystem.get(uri,conf,user);
    }

    /**
     * @After注解标注的方法会自动在@Test标注的方法执行之后执行.
     */
    @After
    public void close() throws Exception{
        fs.close();
    }


    @Test
    public  void testHDFS() throws Exception{
       //1. 获取客户端对象: 文件系统对象
        //URI uri = new URI("hdfs://hadoop102:9820");
        URI uri = URI.create("hdfs://hadoop102:9820");
        Configuration conf = new Configuration();
        String user = "atguigu";

        FileSystem fs = FileSystem.get(uri, conf, user);
        //2. 调用API
        boolean b = fs.mkdirs(new Path("/java"));

        //3. 关闭
        fs.close();
    }
}
