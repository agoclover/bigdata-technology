package com.atguigu.cases.commonfriend;

import com.atguigu.mrutils.BaseDriver;
import com.atguigu.mrutils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/5/27 6:22 下午
 */
public class CommonFriend {
    public static class CFMapper extends Mapper<LongWritable, Text, Text, Text>{
        Text k = new Text();
        Text v = new Text();

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
            String[] split = value.toString().split(":");
            k.set(split[0]);
            String[] names = split[1].split(",");
            for (String friend : names) {
                v.set(friend);
                context.write(k, v);
                context.write(v, k);
            }
        }
    }

    public static class CFReducer extends Reducer<Text, Text, Text, Text>{
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
            HashSet<String> set = new HashSet<>();
            for (Text friend : values) {
                set.add(friend.toString());
            }
            StringBuilder builder = new StringBuilder();
            Iterator<String> iterator = set.iterator();
            while (iterator.hasNext()){
                builder.append(iterator.next() + " ");
            }
            context.write(key, new Text(builder.toString()));
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String[] inPaths = new String[]{"/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/commonfriend"};
        String outPath = "/Users/amos/Desktop/tmp/output/commonfriend";
        Configuration conf = new Configuration();
        String jobName = "common friends";

        JobInitModel job = new JobInitModel(inPaths, outPath, conf, null, jobName, CommonFriend.class,
                null, CFMapper.class, Text.class, Text.class, null, 1, null,
                CFReducer.class, Text.class, Text.class);
        BaseDriver.initJob(job);
    }
}
