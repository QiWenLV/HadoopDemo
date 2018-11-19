package com.zqw.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.zookeeper.txn.Txn;


import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private FileSplit split;
    private Configuration configuration;

    private FSDataOutputStream googlefos;
    private FSDataOutputStream otherfos;

    public FilterRecordWriter(TaskAttemptContext job) {
        Configuration configuration = job.getConfiguration();


        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(configuration);

            //创建两个输出流
            googlefos = fs.create(new Path("f:/b/f/google.log"));
            otherfos = fs.create(new Path("f:/b/f/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断key中是否包含google
        if(text.toString().contains("google")){
            googlefos.write(text.getBytes());
        }else {
            otherfos.write(text.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭资源

        if(googlefos != null){
            googlefos.close();
        }
        if(otherfos != null){
            otherfos.close();
        }
    }
}
