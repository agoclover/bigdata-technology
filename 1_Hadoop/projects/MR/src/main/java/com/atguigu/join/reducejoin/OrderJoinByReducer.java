package com.atguigu.join.reducejoin;

import com.atguigu.join.bean.Order;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/26 7:33 下午
 */
public class OrderJoinByReducer {
    public static class JoinByReducerMapper extends Mapper<LongWritable, Text, Order, NullWritable>{
        private String filename;
        private Order order = new Order();
        /**
         * Called once at the beginning of the task.
         *
         * @param context
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            filename = inputSplit.getPath().getName();
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
            String[] split = value.toString().split("\t");
            if ("pname.txt".equals(filename)){
                order.setId("");
                order.setPid(split[0]);
                order.setAmount(0);
                order.setPname(split[1]);
            } else {
                order.setId(split[0]);
                order.setPid(split[1]);
                order.setAmount(Integer.parseInt(split[2]));
                order.setPname("");
            }
            context.write(order, NullWritable.get());
        }
    }

    public static class JoinByReducerGroupComparator extends WritableComparator{
        public JoinByReducerGroupComparator() {
            super(Order.class, true);
        }

        /**
         * Compare two WritableComparables.
         *
         * <p> The default implementation uses the natural ordering, calling {@link
         * Comparable#compareTo(Object)}.
         *
         * @param a
         * @param b
         */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Order a1 = (Order) a;
            Order b1 = (Order) b;
            return a1.getPid().compareTo(b1.getPid());
        }
    }

    public static class JoinByReducerReducer extends Reducer<Order, NullWritable, Order, NullWritable>{
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
        protected void reduce(Order key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<NullWritable> iterator = values.iterator();
            iterator.next();
            String pname = key.getPname();

            while (iterator.hasNext()){
                iterator.next();
                key.setPname(pname);
                context.write(key, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(OrderJoinByReducer.class);
        job.setMapperClass(JoinByReducerMapper.class);
        job.setReducerClass(JoinByReducerReducer.class);
        job.setMapOutputKeyClass(Order.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Order.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(JoinByReducerGroupComparator.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/amos/Desktop/tmp/data/join"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/amos/Desktop/tmp/output/join"));
        job.waitForCompletion(true);
    }
}
