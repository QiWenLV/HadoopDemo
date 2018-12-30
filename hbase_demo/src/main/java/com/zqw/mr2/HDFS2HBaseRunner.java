package com.zqw.mr2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFS2HBaseRunner implements Tool{
    private Configuration conf = null;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(HDFS2HBaseRunner.class);

        //组装Job
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob("fruit_mr", Write2HBaseReducer.class, job);

        //InputFormat
        FileInputFormat.addInputPath(job, new Path("/input_fruit/"));
        return job.waitForCompletion(true) ? 0:1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new HDFS2HBaseRunner(), args);
        System.out.println(status);
    }
}
