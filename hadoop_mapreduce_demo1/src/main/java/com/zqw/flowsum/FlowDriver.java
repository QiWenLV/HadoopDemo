package com.zqw.flowsum;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1. 获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2. 获取jar包的位置
        job.setJarByClass(FlowDriver.class);

        //3. 指定job要使用的mapper/Reducer的业务类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4. 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5. 指定最终输出的数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6. 指定job输入输出原始文件的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //ps.设置分区
        job.setPartitionerClass(ProvincePartitioner.class);
        //因为设置了5个分区，所以需要设置5个ReduceTask，如果设置一个，则只输出一个文件，如果设置超过5个，则会输出空文件
        //如果设置不足5个(不为1)则报错
        job.setNumReduceTasks(5);

        //7. 将job中配置的相关参数，以及相关的java类所在的jar包交给yarn去运行
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
