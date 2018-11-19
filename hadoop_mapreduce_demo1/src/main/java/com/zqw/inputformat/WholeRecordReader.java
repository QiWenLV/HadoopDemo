package com.zqw.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    BytesWritable value = new BytesWritable();
    boolean isProccess = false;

    FileSplit split;
    Configuration configuration;

    //初始化
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit)inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    //读取每一个文件
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(!isProccess){
            //缓冲区
            byte[] buf = new byte[(int)split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            //获取文件系统
            try {
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);

                //打开输入流
                fis = fs.open(path);

                //拷贝，将流中的数据拷贝的缓冲区中
                IOUtils.readFully(fis, buf, 0, buf.length);

                //缓冲区中的数据最终输出
                value.set(buf, 0, buf.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fs);
            }

            isProccess = true;

            return true;
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProccess ? 1 : 0;
    }

    @Override
    public void close() throws IOException {

    }
}
