package com.zqw.mr1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2FruitMRRunner implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        //创建job
        Job job = Job.getInstance(conf);

        job.setJarByClass(Fruit2FruitMRRunner.class);
        //配置Job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        //配置Mapper
        TableMapReduceUtil.initTableMapperJob("fruit", scan, ReadFruitMapper.class, ImmutableBytesWritable.class, Put.class, job);
        //配置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRReducer.class, job);
        job.setNumReduceTasks(1);

        boolean result = job.waitForCompletion(true);

        return result ? 0: 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new Fruit2FruitMRRunner(), args);
            System.out.println(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
