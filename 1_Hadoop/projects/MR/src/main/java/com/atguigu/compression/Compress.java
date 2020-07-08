package com.atguigu.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * <p>Compress / Decompress</p>
 *
 * <p>压缩操作和解压缩操作的原理:
 * 压缩: 将数据通过支持压缩的流写出;
 * 解压缩: 将压缩的数据通过支持解压缩的流写入.</p>
 *
 * @author Zhang Chao
 * @version mr_day12
 * @date 2020/5/29 3:11 下午
 */
public class Compress {

    /**
     * 压缩本质: 将数据通过支持压缩的流写出;
     * 实测结果: 688.2M --> 69.3M
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Test
    public void testCompressInputStream() throws IOException, ClassNotFoundException {
        String inPath = "/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/web_kpi/web_kpi.log";
        String method = "org.apache.hadoop.io.compress.DefaultCodec";

        //1. 文件输入流
        FileInputStream in = new FileInputStream(inPath);

        //2. 编解码器
        /*
        org.apache.hadoop.mapreduce.lib.input.TextInputFirmat
         */
        Class<?> codecClass = Class.forName(method);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        //3. 输出流
        FileOutputStream fos = new FileOutputStream(inPath + codec.getDefaultExtension());

        //4. 编解码器包装
        CompressionOutputStream out = codec.createOutputStream(fos);

        //5. 流对拷
        IOUtils.copyBytes(in, out, conf);

        //6. 关闭流
        IOUtils.closeStreams(in);
        IOUtils.closeStreams(out);
    }

    @Test
    public void testDecompressOutputStream() throws IOException {
        String inPath = "/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/testCompress/web_kpi.log.deflate";

        //1. 通过输入文件后缀得到编解码器
        Configuration conf = new Configuration();
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path(inPath));

        //2. 输入流
        FileInputStream fis = new FileInputStream(inPath);

        //3. 编解码器包装
        CompressionInputStream in = codec.createInputStream(fis);

        //4. 输出流
        FileOutputStream out = new FileOutputStream("/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/testCompress/web_kpi.log");

        //5. 流拷贝
        IOUtils.copyBytes(in, out, conf);

        //6. 关闭流
        IOUtils.closeStreams(in, out);
    }
}
