package com.atguigu.etl;

import com.atguigu.mrutils.BaseDriver;
import com.atguigu.mrutils.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>web.log 数据的数据清洗 ETL</p>
 *
 * <p>尝试</p>
 *
 * @author Zhang Chao
 * @version mr_day11
 * @date 2020/5/27 2:40 下午
 */
public class WebLogETL {
    public static class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        private Text k = new Text();
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
            // 1 获取1行
            String line = value.toString();

            // 2 解析日志是否合法
            LogBean bean = parseLog(line);

            if (!bean.isValid()) {//不合法直接忽略
                return;
            }
            //将合法的 bean 输出为 Text
            k.set(bean.toString());

            // 3 输出
            context.write(k, NullWritable.get());
        }

        // 解析日志
        private LogBean parseLog(String line) {

            LogBean logBean = new LogBean();

            // 1 截取
            String[] fields = line.split(" ");

            if (fields.length > 11) {

                // 2封装数据
                logBean.setRemote_addr(fields[0]);
                logBean.setRemote_user(fields[1]);
                logBean.setTime_local(fields[3].substring(1));
                logBean.setRequest(fields[6]);
                logBean.setStatus(fields[8]);
                logBean.setBody_bytes_sent(fields[9]);
                logBean.setHttp_referer(fields[10]);

                if (fields.length > 12) {
                    logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
                }else {
                    logBean.setHttp_user_agent(fields[11]);
                }

                // 大于400，HTTP错误
                if (Integer.parseInt(logBean.getStatus()) >= 400) {
                    logBean.setValid(false);
                }
            }else {
                logBean.setValid(false);
            }

            return logBean;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String[] inPaths = new String[]{"/Users/amos/bigdata_learn/1_Hadoop/local_resource/data/web_kpi_demo"};
        String outPath = "/Users/amos/Desktop/tmp/output/web_log_demo";
        Configuration conf = new Configuration();
        String jobName = "web_log_etl_demo";

        JobInitModel job = new JobInitModel(
                inPaths,
                outPath,
                conf,
                null,
                jobName,
                WebLogETL.class,
                null,
                LogMapper.class,
                Text.class,
                NullWritable.class,
                null,
                0,
                null,
                null,
                null,
                null
        );

        BaseDriver.initJob(job);
    }
}
