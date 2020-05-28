package com.atguigu.mr;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p>自定义输出路径</p>
 *
 * <p>通过自定义 XXOutputFormat 和 XXRecordWriter 以自定义输出路径</p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/25 10:25 下午
 */
public class Filter {

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
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
            context.write(value, NullWritable.get());
        }
    }

    public static class FilterRducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        Text k = new Text();
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
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                k.set(key.toString() + "\n");
                context.write(k, value);
            }
        }
    }

    public static class FilterOutputFormat extends FileOutputFormat<Text, NullWritable>{
        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new FilterRecordWriter(job);
        }
    }

    public static class FilterRecordWriter extends RecordWriter<Text, NullWritable>{
        FSDataOutputStream atguigu = null;
        FSDataOutputStream others = null;
        public FilterRecordWriter(TaskAttemptContext job) {
            FileSystem fs;

            try {
                fs = FileSystem.get(job.getConfiguration());

                Path atguiguPath = new Path("/Users/amos/Desktop/tmp/output/atguigu");
                Path othersPath = new Path("/Users/amos/Desktop/tmp/output/others");

                atguigu = fs.create(atguiguPath);
                others = fs.create(othersPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Writes a key/value pair.
         *
         * @param key   the key to write.
         * @param value the value to write.
         * @throws IOException
         */
        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if (key.toString().contains("atguigu")) {
                atguigu.write(key.toString().getBytes());
            } else {
                others.write(key.toString().getBytes());
            }
        }

        /**
         * Close this <code>RecordWriter</code> to future operations.
         *
         * @param context the context of the task
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStreams(atguigu, others);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Filter.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterRducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(FilterOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/amos/Desktop/tmp/data"));

        FileOutputFormat.setOutputPath(job, new Path("/Users/amos/Desktop/tmp/output/status"));

        boolean b = job.waitForCompletion(true);
    }
}
