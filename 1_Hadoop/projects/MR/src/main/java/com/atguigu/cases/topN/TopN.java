package com.atguigu.cases.topN;

import com.atguigu.mr.bean.FlowBean;
import com.atguigu.mrutils.BaseDriver;
import com.atguigu.mrutils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/27 4:56 下午
 */
public class TopN {
    public static class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
        private TreeMap<FlowBean, Text> map = new TreeMap<>();
        private FlowBean bean;
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
            bean = new FlowBean(Long.parseLong(split[1]), Long.parseLong(split[2]));
            map.put(bean, new Text(split[0]));
            if (map.size() > 10) {
                map.remove(map.lastKey());
            }
        }

        /**
         * Called once at the end of the task.
         *
         * @param context
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<FlowBean> keys = map.keySet().iterator();

            while (keys.hasNext()){
                FlowBean key = keys.next();
                context.write(key, map.get(key));
            }
        }
    }

    public static class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
        private TreeMap<FlowBean, Text> map = new TreeMap<>();
        private FlowBean bean;

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
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                bean = new FlowBean(key.getUpFlow(), key.getDownFlow());
                map.put(bean, new Text(value));
                if (map.size()>10){
                    map.remove(map.lastKey());
                }
            }
        }

        /**
         * Called once at the end of the task.
         *
         * @param context
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<FlowBean> beans = map.keySet().iterator();

            while (beans.hasNext()){
                FlowBean bean = beans.next();
                context.write(new Text(map.get(bean)), bean); //这里又 new 了一下
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String[] inPaths = new String[]{"/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/topN"};
        String outpath = "/Users/amos/Desktop/tmp/output/topN";
        Configuration conf = new Configuration();
        String jobName = "topN";

        JobInitModel job = new JobInitModel(inPaths,outpath,conf,null, jobName, TopN.class,
                null, TopNMapper.class, FlowBean.class, Text.class, null,
                1, null, TopNReducer.class, Text.class, FlowBean.class
        );

        BaseDriver.initJob(job);
    }
}
