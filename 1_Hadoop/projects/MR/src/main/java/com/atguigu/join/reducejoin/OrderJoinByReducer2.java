package com.atguigu.join.reducejoin;

import com.atguigu.join.bean.Order2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.BeanUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>OrderJoinByReducer 通过 Text, Order2 传递</p>
 *
 * <p>这里有一点需要注意就是 Reducer 中的传入 k-values 是一个地址的不同变化的数据.
 * 所以需要对数据进行深拷贝, 建议使用 Spring 或 Cglib 的 BeanUtils.copyProperties() 的深拷贝方法</p>
 *
 * @author Zhang Chao
 * @version mr_day11
 * @date 2020/5/27 9:06 上午
 */
public class OrderJoinByReducer2 {

    public static class OrderJoinByReducerMapper2 extends Mapper<LongWritable, Text, Text, Order2>{
        private String filename;
        private Text k = new Text();
        private Order2 v = new Order2();

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
            if (filename.contains("pname")){
                k.set(split[0]);
                v.setId("");
                v.setPid(split[0]);
                v.setAmount(0);
                v.setPname(split[1]);
                v.setFlag("pname");
            } else {
                k.set(split[1]);
                v.setId(split[0]);
                v.setPid(split[1]);
                v.setAmount(Integer.parseInt(split[2]));
                v.setPname("");
                v.setFlag("order");
            }
            context.write(k, v);
        }
    }

    public static class OrderJoinByReducerReducer2 extends Reducer<Text, Order2, Order2, NullWritable>{
        List list = new ArrayList<Order2>();
        Order2 pnameOrder;
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
        protected void reduce(Text key, Iterable<Order2> values, Context context) throws IOException, InterruptedException {
            for (Order2 order : values) {
                Order2 currentOrder = new Order2();
                BeanUtils.copyProperties(order, currentOrder);
                    /*
                    apache 的 BeanUtils.copyProperties() 性能不稳定, 有时候还会出现深拷贝失败, 建议使用 spring 的或 Cglib 的
                    本配置为 spring 的.
                     */
//                    BeanUtils.copyProperties(currentOrder, order);
                if ("order".equals(currentOrder.getFlag())) {
                    list.add(currentOrder);
                } else {
                    pnameOrder = currentOrder;
                }
            }
            for (Object o : list) {
                Order2 o1 = (Order2) o;
                o1.setPname(pnameOrder.getPname());
                context.write(o1, NullWritable.get());
            }
            // 清空集合中的数据
            list.clear();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(OrderJoinByReducer2.class);
        job.setMapperClass(OrderJoinByReducerMapper2.class);
        job.setReducerClass(OrderJoinByReducerReducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Order2.class);
        job.setOutputKeyClass(Order2.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/amos/Desktop/tmp/data/join"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/amos/Desktop/tmp/output/joinbyreducer2"));
        job.waitForCompletion(true);
    }
}
