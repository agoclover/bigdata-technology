package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * <p>Hadoop client api exercise</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version hadoop_day5
 * @date 2020/5/18 10:08 上午
 */
public class HdfsClient {
    private URI uri = URI.create("hdfs://hadoop102:9820");
    private Configuration conf = new Configuration();
    String user = "atguigu";
    FileSystem fs;

    @Test
    public void testDownloadViaIOUtils() throws IOException {
        String src = "/test";
        String des = "/Users/amos/Desktop/tmp/test.txt";

        FSDataInputStream in = fs.open(new Path(src));

        FileOutputStream out = new FileOutputStream(des);

        IOUtils.copyBytes(in, out, new Configuration());
    }

    /**
     * 基于 IOUtils 上传下载文件
     */
    @Test
    public void testUploadViaIOUtils() throws IOException {
        String src = "/Users/amos/Desktop/test.md";
        String des = "/test.md";
        File file = new File(src);
        FileInputStream in = new FileInputStream(file);

        FSDataOutputStream out = fs.create(new Path(des));

        IOUtils.copyBytes(in, out, new Configuration()); //自动close
    }

    /**
     * 测试 printFileAndDir() 方法
     * @throws IOException
     */
    @Test
    public void testPrintFileAndDir() throws IOException {
        printFileAndDir(new Path("/"), fs);
    }

    /**
     * 封装递归查看目录以及文件的方法
     * @param path
     * @param fs
     * @throws IOException
     */
    public void printFileAndDir(Path path, FileSystem fs) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("File: " + fileStatus.getPath());
            } else {
                System.out.println("Dir: " + fileStatus.getPath());
                printFileAndDir(fileStatus.getPath(), fs);
            }
        }
    }

    /**
     * 文件类型
     */
    @Test
    public void testFileTypes() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("File: " + fileStatus.getPath());
            } else {
                System.out.println("Path: " + fileStatus.getPath());
            }
        }

    }

    /**
     * 文件详情查看
     */
    @Test
    public void testFileDetails() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"),
                true);

        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            System.out.println("========== " + fileStatus.getPath().getName() + "==========");
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
     * 测试重命名和移动
     */
    @Test
    public void testRename() throws IOException {
        // 相当于 mv 命令
        boolean b = fs.rename(new Path("/books"),
                new Path("/user/atguigu/data/books"));
    }

    /**
     * 测试文件删除
     */
    @Test
    public void testDelete() throws IOException {
        boolean b = fs.delete(new Path("/user/atguigu/data/books"), true);
    }

    /**
     * 测试文件下载
     * param useRawLocalFileSystem: whether to use RawLocalFileSystem as local file system or not.
     * false 则生成 .crc 校验文件 (linux下get也不会有这个校验文件)
     * true 则不生成校验文件
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false,
                new Path("/user/atguigu/data/books/"),
                new Path("/Users/amos/Desktop/tmp/"),
                false);
    }

    /**
     * 测试文件上传
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(false, true,
//                new Path("/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/web_kpi/web_kpi.log"),
//                new Path("/user/atguigu/data/web_kpi/"));
                // 不能使用 /* 表示所有问价, 直接如下就可以了
                new Path("/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/books"),
                new Path("/user/atguigu/data/books/"));
    }

    @Before
    public void init() throws IOException, InterruptedException {
        fs = FileSystem.get(uri, conf, user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 最基本的连接与操作.
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void test1() throws IOException, InterruptedException, URISyntaxException {
        //1. 创建HDFS客户端对象,传入uri， configuration , user
        FileSystem atguigu = FileSystem.get(new URI("hdfs://hadoop102:9820"), new Configuration(), "atguigu");
        //2. 操作集群
        // 例如：在集群的/目录下创建 testHDFS目录
        boolean b = atguigu.mkdirs(new Path("/testIdea"));
        //3. 关闭资源
        atguigu.close();
    }
}
