package com.atguigu.gulivideo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EtlMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Text  outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一条数据
        String data = value.toString();
        //开始清洗
        String resultData = EtlUtils.etlData(data);

        if(resultData == null){
            //脏数据
            return ;
        }
        outK.set(resultData);
        context.write(outK,NullWritable.get());
    }
}
