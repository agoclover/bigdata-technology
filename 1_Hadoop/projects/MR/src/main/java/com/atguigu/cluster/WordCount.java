package com.atguigu.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p>MR word count 集群端 调用+运行 </p>
 *
 * <p>仿写 word count MapReduce 程序
 * 集群端调用, 集群端运行</p>
 *
 * @author Zhang Chao
 * @version Hadoop_day6
 * @date 2020/5/20 9:52 下午
 */
public class WordCount {

    /**
     * 自定义 XXXMapper 类
     * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);

        /**
         * Mapper 阶段处理业务逻辑
         * @param key KEYIN
         * @param value VALUEIN
         * @param context 上下文对象, 负责整个 Mapper 类中方法的调用.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1. get key-in value
            String line = value.toString();
            // 2. split the line
            String[] words = line.split(" ");
            // 3. map every word into K-V
            for (String wordInLine : words) {
                // 4. write out
                word.set(wordInLine);
                context.write(word, one);
            }
        }
    }

    /**
     * 自定义 XXXReducer 类
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable count = new IntWritable();
        /**
         * Reducer 阶段处理业务逻辑
         * @param key KEYIN, 表示一个单词
         * @param values VALUESIN, 是一个迭代器对象, 表示一个相同的 key 的上一个阶段的所有输出 K-V 对象的封装
         * @param context 上下文对象, 负责整个 Reducer 类中方法的调用.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                int i = value.get();
                sum += i;
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "hive");

        Job job = Job.getInstance(conf);

        //2. link jar
        job.setJarByClass(WordCount.class); // 在集群上运行

        //3. link Mapper & Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4. set out for mapper
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5. ser in-out for reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6. set input-output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //7. submit job
        job.waitForCompletion(true);
    }
}
