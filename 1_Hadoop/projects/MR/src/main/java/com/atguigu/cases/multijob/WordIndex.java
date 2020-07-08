package com.atguigu.cases.multijob;

import com.atguigu.mrutils.BaseDriver;
import com.atguigu.mrutils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/27 4:12 下午
 */
public class WordIndex {
    public static class WIMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private String fileName;
        private Text k = new Text();
        private IntWritable v = new IntWritable(1);
        /**
         * Called once at the beginning of the task.
         *
         * @param context
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fileSplit = (FileSplit) inputSplit;
            fileName = fileSplit.getPath().getName();
        }

        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         *
         * @param key
         * @param value
         * @param context
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                k.set(word + "--" + fileName);
                context.write(k, v);
            }
        }
    }

    public static class WIReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable v = new IntWritable();
        /**
         * This method is called once for each key. Most applications will define
         * their reduce class by overriding this method. The default implementation
         * is an identity function.
         *
         * @param key
         * @param values
         * @param context
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value : values) {
                num += value.get();
            }
            v.set(num);
            context.write(key, v);
        }
    }

    public static class WIMapper2 extends Mapper<LongWritable, Text, Text, Text>{
        private Text k = new Text();
        private Text v = new Text();
        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         *
         * @param key
         * @param value
         * @param context
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("--");
            k.set(split[0]);
            v.set(split[1]);
            context.write(k, v);
        }
    }

    public static class WIReducer2 extends Reducer<Text, Text, Text, Text>{
        private Text v = new Text();
        /**
         * This method is called once for each key. Most applications will define
         * their reduce class by overriding this method. The default implementation
         * is an identity function.
         *
         * @param key
         * @param values
         * @param context
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = "";
            for (Text text : values) {
                String[] split = text.toString().split("\t");
                line += split[0] + "-->" + split[1] + " ";
            }
            v.set(line);
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String[] inPaths = new String[]{"/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/multijob"};
        String outPath = "/Users/amos/Desktop/tmp/output/multijob";
        Configuration conf = new Configuration();
        String jobName = "step";
        JobInitModel job1 = new JobInitModel(inPaths, outPath,conf, null, jobName+"1", WordIndex.class,
                null, WIMapper.class, Text.class, IntWritable.class, null,
                1, null, WIReducer.class,Text.class, IntWritable.class);

        JobInitModel job2 = new JobInitModel(new String[]{outPath + "/part-*"}, outPath + "/part2", conf, null,
                jobName+"2", WordIndex.class, null, WIMapper2.class, Text.class, Text.class, null, 1, null, WIReducer2.class,
                Text.class, Text.class);

        BaseDriver.initJob(job1, job2);
    }
}
