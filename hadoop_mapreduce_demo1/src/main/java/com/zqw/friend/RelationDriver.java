package com.zqw.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RelationDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //2. 获取jar包的位置
        job.setJarByClass(RelationDriver.class);

        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(FriendReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //6. 指定job输入输出原始文件的目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7. 将job中配置的相关参数，以及相关的java类所在的jar包交给yarn去运行
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
