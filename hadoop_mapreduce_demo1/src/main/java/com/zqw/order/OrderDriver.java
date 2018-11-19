package com.zqw.order;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1. 获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2. 获取jar包的位置
        job.setJarByClass(OrderDriver.class);

        //3. 指定job要使用的mapper/Reducer的业务类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //4. 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5. 指定最终输出的数据kv类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6. 指定job输入输出原始文件的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //设置分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        //设置分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        //7. 将job中配置的相关参数，以及相关的java类所在的jar包交给yarn去运行
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
