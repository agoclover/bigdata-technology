package com.atguigu.join.reducejoin;

import com.atguigu.join.bean.Order;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * <p>通过 reduce 端进行 join 操作</p>
 *
 * <p>多个 reducer 中, 个别 reducer 处理的数据特别大, 称为 数据倾斜.
 * 数据倾斜一般会导致某 job 运行时间很长, 导致整个运行时间变长, 还可能产生处理失败导致全局失败的情况.</p>
 *
 * @author Zhang Chao
 * @version mr_day11
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
            /**
             * Counter
             * Get from context. increment(long n) is to set step length.
             */
            context.getCounter("Reducer Join", "Map setup").increment(1);

            // 关键是要获取切片信息以表明此数据来自于那个文件
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

            context.getCounter("Reducer Join", "Map map").increment(1);

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

            context.getCounter("Reducer Join", "Reducer reduce").increment(1);

            Iterator<NullWritable> iterator = values.iterator();
            iterator.next();
            // 这里不用考虑拷贝属性, 因为是得到了字符串属性, 但还是要考虑到这一点
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
        FileOutputFormat.setOutputPath(job, new Path("/Users/amos/Desktop/tmp/output/joinbyreducer1"));
        job.waitForCompletion(true);
    }
}
