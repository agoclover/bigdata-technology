package com.atguigu.mr;

import com.atguigu.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/21 5:50 下午
 */
public class FlowSum {

    public static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
        private Text phone = new Text();
        private FlowBean bean = new FlowBean();
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
            String line = value.toString();

            String[] items= line.split("\t");
            phone.set(items[1]);
            bean.setUpFlow(Long.parseLong(items[items.length - 3]));
            bean.setDownFlow(Long.parseLong(items[items.length - 2]));
            context.write(phone, bean);
        }
    }

    public static class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
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
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long up = 0;
            long down = 0;
            for (FlowBean value : values) {
                up += value.getUpFlow();
                down += value.getDownFlow();
            }
            FlowBean bean = new FlowBean(up, down);
            context.write(key, bean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(FlowSum.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/test/demo2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/amos/Desktop/tmp/output"));

        job.waitForCompletion(true);
    }
}
